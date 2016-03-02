package temp

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/metadata"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"
	"github.com/hashicorp/go-immutable-radix"
)

func init() {
	agro.RegisterMetadataService("temp", NewTemp)
}

type Server struct {
	mut sync.Mutex

	inode map[string]agro.INodeID
	vol   agro.VolumeID

	tree       *iradix.Tree
	volIndex   map[string]agro.VolumeID
	global     agro.GlobalMetadata
	peers      agro.PeerInfoList
	ring       agro.Ring
	newRing    agro.Ring
	openINodes map[string]map[string]*roaring.Bitmap
	deadMap    map[string]*roaring.Bitmap
	chains     map[string]map[agro.INodeRef]agro.INodeRef

	ringListeners []chan agro.Ring
}

type Client struct {
	cfg    agro.Config
	uuid   string
	srv    *Server
	leader bool
}

func NewServer() *Server {
	r, err := ring.CreateRing(&models.Ring{
		Type:    uint32(ring.Empty),
		Version: 1,
	})
	if err != nil {
		panic(err)
	}
	return &Server{
		volIndex: make(map[string]agro.VolumeID),
		tree:     iradix.New(),
		// TODO(barakmich): Allow creating of dynamic GMD via mkfs to the metadata directory.
		global: agro.GlobalMetadata{
			BlockSize:        256,
			DefaultBlockSpec: blockset.MustParseBlockLayerSpec("crc,base"),
			INodeReplication: 2,
		},
		ring:       r,
		inode:      make(map[string]agro.INodeID),
		openINodes: make(map[string]map[string]*roaring.Bitmap),
		chains:     make(map[string]map[agro.INodeRef]agro.INodeRef),
		deadMap:    make(map[string]*roaring.Bitmap),
	}
}

func NewClient(cfg agro.Config, srv *Server) *Client {
	uuid, err := metadata.MakeOrGetUUID("")
	if err != nil {
		return nil
	}
	srv.openINodes[uuid] = make(map[string]*roaring.Bitmap)
	return &Client{
		cfg:  cfg,
		uuid: uuid,
		srv:  srv,
	}
}

func NewTemp(cfg agro.Config) (agro.MetadataService, error) {
	return NewClient(cfg, NewServer()), nil
}

func (t *Client) GlobalMetadata() (agro.GlobalMetadata, error) {
	return t.srv.global, nil
}

func (t *Client) UUID() string {
	return t.uuid
}

func (t *Client) GetLease() (int64, error) { return 1, nil }
func (t *Client) GetPeers() (agro.PeerInfoList, error) {
	return t.srv.peers, nil
}

func (t *Client) RegisterPeer(_ int64, pi *models.PeerInfo) error {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	for i, p := range t.srv.peers {
		if p.UUID == pi.UUID {
			t.srv.peers[i] = pi
			return nil
		}
	}
	t.srv.peers = append(t.srv.peers, pi)
	return nil
}

func (t *Client) CreateVolume(volume string) error {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	if _, ok := t.srv.volIndex[volume]; ok {
		return agro.ErrExists
	}

	tx := t.srv.tree.Txn()

	k := []byte(agro.Path{Volume: volume, Path: "/"}.Key())
	if _, ok := tx.Get(k); !ok {
		tx.Insert(k, (*models.Directory)(nil))
		t.srv.tree = tx.Commit()
		t.srv.vol++
		t.srv.volIndex[volume] = t.srv.vol
	}

	t.srv.chains[volume] = make(map[agro.INodeRef]agro.INodeRef)
	return nil
}

func (t *Client) CommitINodeIndex(vol string) (agro.INodeID, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	t.srv.inode[vol]++
	return t.srv.inode[vol], nil
}

func (t *Client) Mkdir(p agro.Path, md *models.Metadata) error {
	if p.Path == "/" {
		return errors.New("can't create the root directory")
	}

	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	tx := t.srv.tree.Txn()

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

	t.srv.tree = tx.Commit()
	return nil
}

func (t *Client) ChangeDirMetadata(p agro.Path, md *models.Metadata) error {
	if !p.IsDir() {
		return agro.ErrNotDir
	}

	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	tx := t.srv.tree.Txn()

	k := []byte(p.Key())
	v, ok := tx.Get(k)
	if !ok {
		return agro.ErrNotExist
	}
	dir := v.(*models.Directory)
	dir.Metadata = md
	tx.Insert(k, dir)
	t.srv.tree = tx.Commit()
	return nil
}

func (t *Client) Rmdir(p agro.Path) error {
	if p.Path == "/" {
		return errors.New("can't delete the root directory")
	}
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	tx := t.srv.tree.Txn()

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
	t.srv.tree = tx.Commit()
	return nil
}

func (t *Client) debugPrintTree() {
	it := t.srv.tree.Root().Iterator()
	for {
		k, v, ok := it.Next()
		if !ok {
			break
		}
		fmt.Println(string(k), v)
	}
}

func (t *Client) SetFileEntry(p agro.Path, ent *models.FileEntry) error {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	var (
		tx = t.srv.tree.Txn()
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
	t.srv.tree = tx.Commit()
	return nil
}

func (t *Client) GetChainINode(volume string, base agro.INodeRef) (agro.INodeRef, error) {
	vid, err := t.GetVolumeID(volume)
	if err != nil {
		return agro.INodeRef{}, err
	}
	if base.Volume() != vid {
		return agro.INodeRef{}, errors.New("mismatched volume")
	}
	return t.srv.chains[volume][base], nil
}

func (t *Client) SetChainINode(volume string, base agro.INodeRef, was agro.INodeRef, new agro.INodeRef) error {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	cur := t.srv.chains[volume][base]
	if cur != was {
		return agro.ErrCompareFailed
	}
	t.srv.chains[volume][base] = new
	return nil
}

func (t *Client) Getdir(p agro.Path) (*models.Directory, []agro.Path, error) {
	var (
		tx = t.srv.tree.Txn()
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

func (t *Client) GetVolumes() ([]string, error) {
	var (
		iter = t.srv.tree.Root().Iterator()
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

func (t *Client) GetVolumeName(vid agro.VolumeID) (string, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	for k, v := range t.srv.volIndex {
		if v == vid {
			return k, nil
		}
	}
	return "", errors.New("temp: no such volume exists")

}

func (t *Client) GetVolumeID(volume string) (agro.VolumeID, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	if vol, ok := t.srv.volIndex[volume]; ok {
		return vol, nil
	}
	return 0, errors.New("temp: no such volume exists")
}

func (t *Client) GetRing() (agro.Ring, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	return t.srv.ring, nil
}

func (t *Client) SubscribeNewRings(ch chan agro.Ring) {
	t.srv.SubscribeNewRings(ch)
}

func (t *Client) UnsubscribeNewRings(ch chan agro.Ring) {
	t.srv.UnsubscribeNewRings(ch)
}

func (s *Server) SubscribeNewRings(ch chan agro.Ring) {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.ringListeners = append(s.ringListeners, ch)
}

func (s *Server) UnsubscribeNewRings(ch chan agro.Ring) {
	s.mut.Lock()
	defer s.mut.Unlock()
	for i, c := range s.ringListeners {
		if ch == c {
			s.ringListeners = append(s.ringListeners[:i], s.ringListeners[i+1:]...)
			return
		}
	}
	panic("couldn't remove channel")
}

func (t *Client) Close() error {
	delete(t.srv.openINodes, t.uuid)
	return nil
}

func (t *Client) ClaimVolumeINodes(_ int64, vol agro.VolumeID, inodes *roaring.Bitmap) error {
	volume, _ := t.GetVolumeName(vol)
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	t.srv.openINodes[t.uuid][volume] = inodes
	return nil
}

func (t *Client) ModifyDeadMap(vol agro.VolumeID, live *roaring.Bitmap, dead *roaring.Bitmap) error {
	volume, _ := t.GetVolumeName(vol)
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	x, ok := t.srv.deadMap[volume]
	if !ok {
		x = roaring.NewBitmap()
	}
	x.Or(dead)
	x.AndNot(live)
	t.srv.deadMap[volume] = x
	return nil
}

func (t *Client) GetVolumeLiveness(vol agro.VolumeID) (*roaring.Bitmap, []*roaring.Bitmap, error) {
	volume, _ := t.GetVolumeName(vol)
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	x, ok := t.srv.deadMap[volume]
	if !ok {
		x = roaring.NewBitmap()
	}
	var l []*roaring.Bitmap
	for _, perclient := range t.srv.openINodes {
		if c, ok := perclient[volume]; ok {
			if c != nil {
				l = append(l, c)
			}
		}
	}
	return x, l, nil
}

func (t *Client) SetRing(ring agro.Ring) error {
	return t.srv.SetRing(ring)
}

func (s *Server) SetRing(ring agro.Ring) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.ring = ring
	for _, c := range s.ringListeners {
		c <- s.ring
	}
	return nil
}

func (t *Client) GetINodeIndex(volume string) (agro.INodeID, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	return t.srv.inode[volume], nil
}

func (t *Client) GetINodeIndexes() (map[string]agro.INodeID, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	out := make(map[string]agro.INodeID)
	for k, v := range t.srv.inode {
		out[k] = v
	}
	return out, nil
}

func (s *Server) Close() error {
	return nil
}

func (t *Client) WithContext(_ context.Context) agro.MetadataService {
	return t
}
