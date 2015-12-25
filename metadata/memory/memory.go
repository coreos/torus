package memory

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/blockset"
	"github.com/barakmich/agro/metadata"
	"github.com/barakmich/agro/models"
	"github.com/barakmich/agro/ring"
	"github.com/coreos/pkg/capnslog"
	"github.com/hashicorp/go-immutable-radix"
)

var clog = capnslog.NewPackageLogger("github.com/barakmich/agro", "memory")

func init() {
	agro.RegisterMetadataService("memory", newMemoryMetadata)
}

type memory struct {
	mut sync.Mutex

	inode agro.INodeID
	vol   agro.VolumeID

	tree     *iradix.Tree
	volIndex map[string]agro.VolumeID
	global   agro.GlobalMetadata
	cfg      agro.Config
	uuid     string
}

func newMemoryMetadata(cfg agro.Config) (agro.MetadataService, error) {
	uuid, err := metadata.MakeOrGetUUID(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	t, err := parseFromFile(cfg)
	if err == nil {
		t.uuid = uuid
		return t, nil
	}
	clog.Info("single: couldn't parse metadata: ", err)
	return &memory{
		volIndex: make(map[string]agro.VolumeID),
		tree:     iradix.New(),
		// TODO(barakmich): Allow creating of dynamic GMD via mkfs to the metadata directory.
		global: agro.GlobalMetadata{
			BlockSize:        8 * 1024,
			DefaultBlockSpec: blockset.MustParseBlockLayerSpec("crc,base"),
		},
		cfg:  cfg,
		uuid: uuid,
	}, nil
}

func (s *memory) GlobalMetadata() (agro.GlobalMetadata, error) {
	return s.global, nil
}

func (s *memory) UUID() string {
	return s.uuid
}

func (s *memory) GetPeers() ([]*models.PeerInfo, error) {
	return []*models.PeerInfo{
		&models.PeerInfo{
			UUID:        s.uuid,
			LastSeen:    time.Now().UnixNano(),
			TotalBlocks: s.cfg.StorageSize / s.global.BlockSize,
		},
	}, nil
}

func (s *memory) RegisterPeer(_ *models.PeerInfo) error { return nil }

func (s *memory) CreateVolume(volume string) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	if _, ok := s.volIndex[volume]; ok {
		return agro.ErrExists
	}

	tx := s.tree.Txn()

	k := []byte(agro.Path{Volume: volume, Path: "/"}.Key())
	if _, ok := tx.Get(k); !ok {
		tx.Insert(k, (*models.Directory)(nil))
		s.tree = tx.Commit()
		s.vol++
		s.volIndex[volume] = s.vol
	}

	// TODO(jzelinskie): maybe raise volume already exists
	return nil
}

func (s *memory) CommitInodeIndex() (agro.INodeID, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.inode++
	return s.inode, nil
}

func (s *memory) Mkdir(p agro.Path, dir *models.Directory) error {
	if p.Path == "/" {
		return errors.New("can't create the root directory")
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	tx := s.tree.Txn()

	k := []byte(p.Key())
	if _, ok := tx.Get(k); ok {
		return &os.PathError{
			Op:   "mkdir",
			Path: p.Path,
			Err:  os.ErrExist,
		}
	}
	tx.Insert(k, dir)

	for {
		p.Path, _ = path.Split(strings.TrimSuffix(p.Path, "/"))
		if p.Path == "" {
			break
		}
		k = []byte(p.Key())
		if _, ok := tx.Get(k); !ok {
			return &os.PathError{
				Op:   "stat",
				Path: p.Path,
				Err:  os.ErrNotExist,
			}
		}
	}

	s.tree = tx.Commit()
	return nil
}

func (s *memory) debugPrintTree() {
	it := s.tree.Root().Iterator()
	for {
		k, v, ok := it.Next()
		if !ok {
			break
		}
		clog.Debug(string(k), v)
	}
}

func (s *memory) SetFileINode(p agro.Path, ref agro.INodeRef) error {
	vid, err := s.GetVolumeID(p.Volume)
	if err != nil {
		return err
	}
	if vid != ref.Volume {
		return errors.New("temp: inodeRef volume not for given path volume")
	}
	s.mut.Lock()
	defer s.mut.Unlock()
	var (
		tx = s.tree.Txn()
		k  = []byte(p.Key())
	)
	v, ok := tx.Get(k)
	if !ok {
		return &os.PathError{
			Op:   "stat",
			Path: p.Path,
			Err:  os.ErrNotExist,
		}
	}
	dir := v.(*models.Directory)
	if dir == nil {
		dir = &models.Directory{}
	}
	if dir.Files == nil {
		dir.Files = make(map[string]uint64)
	}
	dir.Files[p.Filename()] = uint64(ref.INode)
	tx.Insert(k, dir)
	s.tree = tx.Commit()
	return nil
}

func (s *memory) Getdir(p agro.Path) (*models.Directory, []agro.Path, error) {
	var (
		tx = s.tree.Txn()
		k  = []byte(p.Key())
	)
	v, ok := tx.Get(k)
	if !ok {
		return nil, nil, &os.PathError{
			Op:   "stat",
			Path: p.Path,
			Err:  os.ErrNotExist,
		}
	}

	var (
		dir     = v.(*models.Directory)
		prefix  = []byte(p.SubdirsPrefix())
		subdirs []agro.Path
	)
	tx.Root().WalkPrefix(prefix, func(k []byte, v interface{}) bool {
		subdirs = append(subdirs, agro.Path{
			Volume: p.Volume,
			Path:   fmt.Sprintf("%s%s", p.Path, bytes.TrimPrefix(k, prefix)),
		})
		return false
	})
	return dir, subdirs, nil
}

func (s *memory) GetVolumes() ([]string, error) {
	var (
		iter = s.tree.Root().Iterator()
		out  []string
		last string
	)
	for {
		k, _, ok := iter.Next()
		if !ok {
			break
		}
		if i := bytes.IndexByte(k, ':'); i != -1 {
			vol := string(k[:i])
			if vol == last {
				continue
			}
			out = append(out, vol)
			last = vol
		}
	}
	return out, nil
}

func (s *memory) GetVolumeID(volume string) (agro.VolumeID, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if vol, ok := s.volIndex[volume]; ok {
		return vol, nil
	}
	return 0, errors.New("temp: no such volume exists")
}

func (s *memory) GetRing() (agro.Ring, error) {
	return ring.CreateRing(&models.Ring{
		Type:    uint32(ring.Single),
		Version: 1,
		UUIDs:   []string{s.uuid},
	})
}

func (s *memory) SubscribeNewRings(ch chan agro.Ring) {
	close(ch)
}

func (s *memory) UnsubscribeNewRings(ch chan agro.Ring) {
	// Kay. We unsubscribed you already.
}

func (s *memory) write() error {
	if s.cfg.DataDir == "" {
		return nil
	}
	outfile := filepath.Join(s.cfg.DataDir, "metadata", "temp.txt")
	clog.Info("writing metadata to file:", outfile)
	f, err := os.Create(outfile)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := bufio.NewWriter(f)
	buf.WriteString(fmt.Sprintf("%d %d\n", s.inode, s.vol))
	b, err := json.Marshal(s.volIndex)
	if err != nil {
		return err
	}
	buf.WriteString(string(b))
	buf.WriteRune('\n')
	b, err = json.Marshal(s.global)
	if err != nil {
		return err
	}
	buf.WriteString(string(b))
	buf.WriteRune('\n')
	it := s.tree.Root().Iterator()
	for {
		k, v, ok := it.Next()
		if !ok {
			break
		}
		buf.WriteString(string(k))
		buf.WriteRune('\n')
		b, err := json.Marshal(v.(*models.Directory))
		if err != nil {
			return err
		}
		buf.WriteString(string(b))
		buf.WriteRune('\n')
	}
	return buf.Flush()
}

func (s *memory) Close() error {
	return s.write()
}

func (s *memory) WithContext(_ context.Context) agro.MetadataService {
	return s
}

func parseFromFile(cfg agro.Config) (*memory, error) {
	s := memory{}
	if cfg.DataDir == "" {
		return nil, os.ErrNotExist
	}
	outfile := filepath.Join(cfg.DataDir, "metadata", "temp.txt")
	if _, err := os.Stat(outfile); err == os.ErrNotExist {
		return nil, os.ErrNotExist
	}
	f, err := os.Open(outfile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := bufio.NewReader(f)
	line, err := buf.ReadString('\n')
	if err != nil {
		return nil, err
	}
	_, err = fmt.Sscanf(line, "%d %d", &s.inode, &s.vol)
	if err != nil {
		return nil, err
	}
	b, err := buf.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &s.volIndex)
	if err != nil {
		return nil, err
	}
	b, err = buf.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &s.global)
	if err != nil {
		return nil, err
	}
	s.tree = iradix.New()
	tx := s.tree.Txn()
	for {
		k, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		k = k[:len(k)-1]
		vbytes, err := buf.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		v := models.Directory{}
		err = json.Unmarshal(vbytes, &v)
		if err != nil {
			return nil, err
		}
		tx.Insert(k, &v)
	}

	s.tree = tx.Commit()
	s.cfg = cfg
	return &s, nil
}
