package temp

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
	"github.com/hashicorp/go-immutable-radix"
)

func init() {
	agro.RegisterMetadataService("temp", newTempMetadata)
}

type temp struct {
	mut sync.Mutex

	inode agro.INodeID
	vol   agro.VolumeID

	tree     *iradix.Tree
	volIndex map[string]agro.VolumeID
	global   agro.GlobalMetadata
	cfg      agro.Config
	uuid     string
}

func newTempMetadata(cfg agro.Config) (agro.MetadataService, error) {
	uuid, err := metadata.MakeOrGetUUID(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	t, err := parseFromFile(cfg)
	if err == nil {
		t.uuid = uuid
		return t, nil
	}
	fmt.Println("temp: couldn't parse metadata: ", err)
	return &temp{
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

func (t *temp) GlobalMetadata() (agro.GlobalMetadata, error) {
	return t.global, nil
}

func (t *temp) UUID() string {
	return t.uuid
}

func (t *temp) GetPeers() ([]*models.PeerInfo, error) {
	return []*models.PeerInfo{
		&models.PeerInfo{
			UUID:        t.uuid,
			LastSeen:    time.Now().UnixNano(),
			TotalBlocks: t.cfg.StorageSize / t.global.BlockSize,
		},
	}, nil
}

func (t *temp) RegisterPeer(_ *models.PeerInfo) error { return nil }

func (t *temp) CreateVolume(volume string) error {
	t.mut.Lock()
	defer t.mut.Unlock()
	if _, ok := t.volIndex[volume]; ok {
		return agro.ErrExists
	}

	tx := t.tree.Txn()

	k := []byte(agro.Path{Volume: volume, Path: "/"}.Key())
	if _, ok := tx.Get(k); !ok {
		tx.Insert(k, (*models.Directory)(nil))
		t.tree = tx.Commit()
		t.vol++
		t.volIndex[volume] = t.vol
	}

	// TODO(jzelinskie): maybe raise volume already exists
	return nil
}

func (t *temp) CommitInodeIndex() (agro.INodeID, error) {
	t.mut.Lock()
	defer t.mut.Unlock()

	t.inode++
	return t.inode, nil
}

func (t *temp) Mkdir(p agro.Path, dir *models.Directory) error {
	if p.Path == "/" {
		return errors.New("can't create the root directory")
	}

	t.mut.Lock()
	defer t.mut.Unlock()

	tx := t.tree.Txn()

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

	t.tree = tx.Commit()
	return nil
}

func (t *temp) debugPrintTree() {
	it := t.tree.Root().Iterator()
	for {
		k, v, ok := it.Next()
		if !ok {
			break
		}
		fmt.Println(string(k), v)
	}
}

func (t *temp) SetFileINode(p agro.Path, ref agro.INodeRef) error {
	vid, err := t.GetVolumeID(p.Volume)
	if err != nil {
		return err
	}
	if vid != ref.Volume {
		return errors.New("temp: inodeRef volume not for given path volume")
	}
	t.mut.Lock()
	defer t.mut.Unlock()
	var (
		tx = t.tree.Txn()
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
	t.tree = tx.Commit()
	return nil
}

func (t *temp) Getdir(p agro.Path) (*models.Directory, []agro.Path, error) {
	var (
		tx = t.tree.Txn()
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

func (t *temp) GetVolumes() ([]string, error) {
	var (
		iter = t.tree.Root().Iterator()
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

func (t *temp) GetVolumeID(volume string) (agro.VolumeID, error) {
	t.mut.Lock()
	defer t.mut.Unlock()

	if vol, ok := t.volIndex[volume]; ok {
		return vol, nil
	}
	return 0, errors.New("temp: no such volume exists")
}

func (t *temp) GetRing() (agro.Ring, error) {
	return ring.CreateRing(&models.Ring{
		Type:    uint32(ring.Single),
		Version: 1,
		UUIDs:   []string{t.uuid},
	})
}

func (t *temp) SubscribeNewRings(ch chan agro.Ring) {
	close(ch)
}

func (t *temp) UnsubscribeNewRings(ch chan agro.Ring) {
	// Kay. We unsubscribed you already.
}

func (t *temp) write() error {
	if t.cfg.DataDir == "" {
		return nil
	}
	outfile := filepath.Join(t.cfg.DataDir, "metadata", "temp.txt")
	fmt.Println("writing metadata to file:", outfile)
	f, err := os.Create(outfile)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := bufio.NewWriter(f)
	buf.WriteString(fmt.Sprintf("%d %d\n", t.inode, t.vol))
	b, err := json.Marshal(t.volIndex)
	if err != nil {
		return err
	}
	buf.WriteString(string(b))
	buf.WriteRune('\n')
	b, err = json.Marshal(t.global)
	if err != nil {
		return err
	}
	buf.WriteString(string(b))
	buf.WriteRune('\n')
	it := t.tree.Root().Iterator()
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

func (t *temp) Close() error {
	return t.write()
}

func (t *temp) WithContext(_ context.Context) agro.MetadataService {
	return t
}

func parseFromFile(cfg agro.Config) (*temp, error) {
	t := temp{}
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
	_, err = fmt.Sscanf(line, "%d %d", &t.inode, &t.vol)
	if err != nil {
		return nil, err
	}
	b, err := buf.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &t.volIndex)
	if err != nil {
		return nil, err
	}
	b, err = buf.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &t.global)
	if err != nil {
		return nil, err
	}
	t.tree = iradix.New()
	tx := t.tree.Txn()
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

	t.tree = tx.Commit()
	t.cfg = cfg
	return &t, nil
}
