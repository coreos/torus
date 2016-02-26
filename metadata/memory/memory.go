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

	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/metadata"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"
	"github.com/coreos/pkg/capnslog"
	"github.com/hashicorp/go-immutable-radix"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "memory")

func init() {
	agro.RegisterMetadataService("memory", newMemoryMetadata)
}

type memory struct {
	mut sync.Mutex

	inodes map[string]agro.INodeID
	vol    agro.VolumeID

	tree         *iradix.Tree
	volIndex     map[string]agro.VolumeID
	global       agro.GlobalMetadata
	cfg          agro.Config
	uuid         string
	openINodes   map[string]*roaring.Bitmap
	deadMap      map[string]*roaring.Bitmap
	ring         agro.Ring
	ringWatchers []chan agro.Ring
	chains       map[string]map[agro.INodeRef]agro.INodeRef
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
	ring, err := ring.CreateRing(&models.Ring{
		Type:    uint32(ring.Single),
		Version: 1,
		Peers: []*models.PeerInfo{
			&models.PeerInfo{
				UUID: t.uuid,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	clog.Info("single: couldn't parse metadata: ", err)
	return &memory{
		volIndex: make(map[string]agro.VolumeID),
		tree:     iradix.New(),
		inodes:   make(map[string]agro.INodeID),
		// TODO(barakmich): Allow creating of dynamic GMD via mkfs to the metadata directory.
		global: agro.GlobalMetadata{
			BlockSize:        8 * 1024,
			DefaultBlockSpec: blockset.MustParseBlockLayerSpec("crc,base"),
		},
		cfg:        cfg,
		uuid:       uuid,
		openINodes: make(map[string]*roaring.Bitmap),
		deadMap:    make(map[string]*roaring.Bitmap),
		ring:       ring,
		chains:     make(map[string]map[agro.INodeRef]agro.INodeRef),
	}, nil
}

func (s *memory) GlobalMetadata() (agro.GlobalMetadata, error) {
	return s.global, nil
}

func (s *memory) UUID() string {
	return s.uuid
}

func (s *memory) GetPeers() (agro.PeerInfoList, error) {
	return agro.PeerInfoList{
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
	s.chains[volume] = make(map[agro.INodeRef]agro.INodeRef)
	return nil
}

func (s *memory) CommitINodeIndex(vol string) (agro.INodeID, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.inodes[vol]++
	return s.inodes[vol], nil
}

func (s *memory) GetINodeIndex(volume string) (agro.INodeID, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.inodes[volume], nil
}

func (s *memory) GetINodeIndexes() (map[string]agro.INodeID, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	out := make(map[string]agro.INodeID)
	for k, v := range s.inodes {
		out[k] = v
	}
	return out, nil
}

func (s *memory) Mkdir(p agro.Path, md *models.Metadata) error {
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
	tx.Insert(k, &models.Directory{
		Metadata: md,
		Files:    make(map[string]*models.FileEntry),
	})

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

func (s *memory) Rmdir(p agro.Path) error {
	if p.Path == "/" {
		return errors.New("can't delete the root directory")
	}
	s.mut.Lock()
	defer s.mut.Unlock()

	tx := s.tree.Txn()

	k := []byte(p.Key())
	v, ok := tx.Get(k)
	if !ok {
		return &os.PathError{
			Op:   "rmdir",
			Path: p.Path,
			Err:  os.ErrNotExist,
		}
	}
	dir := v.(*models.Directory)
	if len(dir.Files) != 0 {
		return &os.PathError{
			Op:   "rmdir",
			Path: p.Path,
			Err:  os.ErrInvalid,
		}
	}
	tx.Delete(k)
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

func (s *memory) SetFileEntry(p agro.Path, ent *models.FileEntry) error {
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
		dir.Files = make(map[string]*models.FileEntry)
	}
	if ent.Chain == 0 && ent.Sympath == "" {
		delete(dir.Files, p.Filename())
	} else {
		dir.Files[p.Filename()] = ent
	}
	tx.Insert(k, dir)
	s.tree = tx.Commit()
	return nil
}

func (s *memory) GetChainINode(volume string, base agro.INodeRef) (agro.INodeRef, error) {
	vid, err := s.GetVolumeID(volume)
	if err != nil {
		return agro.INodeRef{}, err
	}
	if base.Volume() != vid {
		return agro.INodeRef{}, errors.New("mismatched volume")
	}
	return s.chains[volume][base], nil
}

func (s *memory) SetChainINode(volume string, base agro.INodeRef, was agro.INodeRef, new agro.INodeRef) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	cur := s.chains[volume][base]
	if cur != was {
		return agro.ErrCompareFailed
	}
	s.chains[volume][base] = new
	return nil
}

func (s *memory) Getdir(p agro.Path) (*models.Directory, []agro.Path, error) {
	var (
		tx = s.tree.Txn()
		k  = []byte(p.Key())
	)
	v, ok := tx.Get(k)
	if v == nil || v.(*models.Directory) == nil || !ok {
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

func (s *memory) ChangeDirMetadata(p agro.Path, md *models.Metadata) error {
	if !p.IsDir() {
		return agro.ErrNotDir
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	tx := s.tree.Txn()

	k := []byte(p.Key())
	v, ok := tx.Get(k)
	if !ok {
		return agro.ErrNotExist
	}
	dir := v.(*models.Directory)
	dir.Metadata = md
	tx.Insert(k, dir)
	s.tree = tx.Commit()
	return nil
}

func (s *memory) GetVolumes() ([]string, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

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

func (s *memory) GetVolumeName(vid agro.VolumeID) (string, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	for k, v := range s.volIndex {
		if v == vid {
			return k, nil
		}
	}
	return "", errors.New("memory: no such volume exists")

}

func (s *memory) GetVolumeID(volume string) (agro.VolumeID, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if vol, ok := s.volIndex[volume]; ok {
		return vol, nil
	}
	return 0, errors.New("memory: no such volume exists")
}

func (s *memory) GetRing() (agro.Ring, error) {
	return s.ring, nil
}

func (s *memory) SubscribeNewRings(ch chan agro.Ring) {
	s.ringWatchers = append(s.ringWatchers, ch)
}

func (s *memory) UnsubscribeNewRings(ch chan agro.Ring) {
	for i, c := range s.ringWatchers {
		if c == ch {
			s.ringWatchers = append(s.ringWatchers[:i], s.ringWatchers[i+1:]...)
		}
	}
}

func (s *memory) SetRing(newring agro.Ring) error {
	if newring.Type() != ring.Single {
		return errors.New("invalid ring type for memory mds (must be Single)")
	}
	s.ring = newring
	for _, c := range s.ringWatchers {
		c <- newring
	}
	return nil
}

func (s *memory) ClaimVolumeINodes(vol agro.VolumeID, inodes *roaring.Bitmap) error {
	volume, _ := s.GetVolumeName(vol)
	s.openINodes[volume] = inodes
	return nil
}

func (s *memory) ModifyDeadMap(vol agro.VolumeID, live *roaring.Bitmap, dead *roaring.Bitmap) error {
	volume, _ := s.GetVolumeName(vol)
	x, ok := s.deadMap[volume]
	if !ok {
		x = roaring.NewBitmap()
	}
	x.Or(dead)
	x.AndNot(live)
	s.deadMap[volume] = x
	return nil
}

func (s *memory) GetVolumeLiveness(vol agro.VolumeID) (*roaring.Bitmap, []*roaring.Bitmap, error) {
	volume, _ := s.GetVolumeName(vol)
	x, ok := s.deadMap[volume]
	if !ok {
		x = roaring.NewBitmap()
	}
	var l []*roaring.Bitmap
	if y, ok := s.openINodes[volume]; ok {
		if y != nil {
			l = append(l, y)
		}
	}
	return x, l, nil
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
	buf.WriteString(fmt.Sprintf("%d\n", s.vol))
	b, err := json.Marshal(s.inodes)
	if err != nil {
		return err
	}
	buf.WriteString(string(b))
	buf.WriteRune('\n')
	b, err = json.Marshal(s.volIndex)
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

// TODO(barakmich): Marshal the deadMap
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
	_, err = fmt.Sscanf(line, "%d", &s.vol)
	if err != nil {
		return nil, err
	}
	b, err := buf.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &s.inodes)
	if err != nil {
		return nil, err
	}
	b, err = buf.ReadBytes('\n')
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
