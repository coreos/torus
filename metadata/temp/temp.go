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

	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/metadata"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"
	"github.com/hashicorp/go-immutable-radix"
	"github.com/tgruben/roaring"
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
	peers      []*models.PeerInfo
	ring       agro.Ring
	newRing    agro.Ring
	openINodes map[string]map[string]*roaring.RoaringBitmap
	deadMap    map[string]*roaring.RoaringBitmap

	ringListeners      []chan agro.Ring
	rebalanceListeners []chan *models.RebalanceStatus
	masterListener     chan *models.RebalanceStatus
	rebalanceSnapshot  *models.RebalanceSnapshot
}

type Client struct {
	cfg    agro.Config
	uuid   string
	srv    *Server
	toC    chan *models.RebalanceStatus
	fromC  chan *models.RebalanceStatus
	master bool
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
		openINodes: make(map[string]map[string]*roaring.RoaringBitmap),
		deadMap:    make(map[string]*roaring.RoaringBitmap),
	}
}

func NewClient(cfg agro.Config, srv *Server) *Client {
	uuid, err := metadata.MakeOrGetUUID("")
	if err != nil {
		return nil
	}
	srv.openINodes[uuid] = make(map[string]*roaring.RoaringBitmap)
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

func (t *Client) GetPeers() ([]*models.PeerInfo, error) {
	return t.srv.peers, nil
}

func (t *Client) RegisterPeer(pi *models.PeerInfo) error {
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

	// TODO(jzelinskie): maybe raise volume already exists
	return nil
}

func (t *Client) CommitINodeIndex(vol string) (agro.INodeID, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	t.srv.inode[vol]++
	return t.srv.inode[vol], nil
}

func (t *Client) Mkdir(p agro.Path, dir *models.Directory) error {
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

func (t *Client) SetFileINode(p agro.Path, ref agro.INodeRef) (agro.INodeID, error) {
	old := agro.INodeID(0)
	vid, err := t.GetVolumeID(p.Volume)
	if err != nil {
		return old, err
	}
	if vid != ref.Volume {
		return old, errors.New("temp: inodeRef volume not for given path volume")
	}
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	var (
		tx = t.srv.tree.Txn()
		k  = []byte(p.Key())
	)
	v, ok := tx.Get(k)
	if !ok {
		return old, &os.PathError{
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
	if v, ok := dir.Files[p.Filename()]; ok {
		old = agro.INodeID(v)
	}
	dir.Files[p.Filename()] = uint64(ref.INode)
	tx.Insert(k, dir)
	t.srv.tree = tx.Commit()
	return old, nil
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

func (t *Client) ClaimVolumeINodes(volume string, inodes *roaring.RoaringBitmap) error {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	t.srv.openINodes[t.uuid][volume] = inodes
	return nil
}

func (t *Client) ModifyDeadMap(volume string, live *roaring.RoaringBitmap, dead *roaring.RoaringBitmap) error {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	x, ok := t.srv.deadMap[volume]
	if !ok {
		x = roaring.NewRoaringBitmap()
	}
	x.Or(dead)
	x.AndNot(live)
	t.srv.deadMap[volume] = x
	return nil
}

func (t *Client) GetVolumeLiveness(volume string) (*roaring.RoaringBitmap, []*roaring.RoaringBitmap, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	x, ok := t.srv.deadMap[volume]
	if !ok {
		x = roaring.NewRoaringBitmap()
	}
	var l []*roaring.RoaringBitmap
	for _, perclient := range t.srv.openINodes {
		if c, ok := perclient[volume]; ok {
			l = append(l, c)
		}
	}
	return x, l, nil
}

func (t *Client) SetRebalanceSnapshot(x *models.RebalanceSnapshot) error {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	t.srv.rebalanceSnapshot = x
	return nil
}

func (t *Client) GetRebalanceSnapshot() (*models.RebalanceSnapshot, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	return t.srv.rebalanceSnapshot, nil
}

func (t *Client) SetRing(ring agro.Ring, force bool) error {
	return t.srv.SetRing(ring, force)
}

func (s *Server) SetRing(ring agro.Ring, force bool) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	if force {
		s.ring = ring
		return nil
	}
	s.newRing = ring
	for _, c := range s.ringListeners {
		c <- s.newRing
	}
	return nil
}

func (t *Client) OpenRebalanceChannels() (inOut [2]chan *models.RebalanceStatus, master bool, err error) {
	t.toC = make(chan *models.RebalanceStatus)
	t.fromC = make(chan *models.RebalanceStatus)
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	isMaster := false
	if t.srv.masterListener == nil {
		isMaster = true
		t.srv.masterListener = t.toC
	}
	t.srv.rebalanceListeners = append(t.srv.rebalanceListeners, t.toC)
	go func(master bool, t *Client) {
		for {
			d, ok := <-t.fromC
			if !ok {
				t.srv.mut.Lock()
				if master {
					t.srv.masterListener = nil
				}
				for i, c := range t.srv.rebalanceListeners {
					if t.toC == c {
						t.srv.rebalanceListeners = append(t.srv.rebalanceListeners[:i], t.srv.rebalanceListeners[i+1:]...)
						break
					}
				}
				t.srv.mut.Unlock()
				close(t.toC)
				return
			}
			t.srv.mut.Lock()
			if master {
				for _, c := range t.srv.rebalanceListeners {
					c <- d
				}
			} else {
				t.srv.masterListener <- d
			}
			t.srv.mut.Unlock()
		}
	}(isMaster, t)
	return [2]chan *models.RebalanceStatus{t.toC, t.fromC}, isMaster, nil
}

func (s *Server) Close() error {
	return nil
}

func (t *Client) WithContext(_ context.Context) agro.MetadataService {
	return t
}
