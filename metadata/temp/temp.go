package temp

import (
	"errors"
	"sync"

	"golang.org/x/net/context"

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

	inode map[agro.VolumeID]agro.INodeID
	vol   agro.VolumeID

	tree     *iradix.Tree
	volIndex map[string]*models.Volume
	global   agro.GlobalMetadata
	peers    agro.PeerInfoList
	ring     agro.Ring
	newRing  agro.Ring

	keys map[string]interface{}

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
		volIndex: make(map[string]*models.Volume),
		tree:     iradix.New(),
		// TODO(barakmich): Allow creating of dynamic GMD via mkfs to the metadata directory.
		global: agro.GlobalMetadata{
			BlockSize:        256,
			DefaultBlockSpec: blockset.MustParseBlockLayerSpec("crc,base"),
			INodeReplication: 2,
		},
		ring:  r,
		keys:  make(map[string]interface{}),
		inode: make(map[agro.VolumeID]agro.INodeID),
	}
}

func NewClient(cfg agro.Config, srv *Server) *Client {
	uuid, err := metadata.MakeOrGetUUID("")
	if err != nil {
		return nil
	}
	return &Client{
		cfg:  cfg,
		uuid: uuid,
		srv:  srv,
	}
}

func NewTemp(cfg agro.Config) (agro.MetadataService, error) {
	return NewClient(cfg, NewServer()), nil
}

func (t *Client) Kind() agro.MetadataKind {
	return agro.TempMetadata
}

func (t *Client) GlobalMetadata() (agro.GlobalMetadata, error) {
	return t.srv.global, nil
}

func (t *Client) UUID() string {
	return t.uuid
}

func (t *Client) GetLease() (int64, error) { return 1, nil }
func (t *Client) GetPeers() (agro.PeerInfoList, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
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

func (t *Client) NewVolumeID() (agro.VolumeID, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	t.srv.vol++
	return t.srv.vol, nil
}

func (t *Client) CommitINodeIndex(vol agro.VolumeID) (agro.INodeID, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	t.srv.inode[vol]++
	return t.srv.inode[vol], nil
}

func (t *Client) GetVolumes() ([]*models.Volume, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	var out []*models.Volume

	for _, v := range t.srv.volIndex {
		out = append(out, v)
	}
	return out, nil
}

func (t *Client) GetVolume(volume string) (*models.Volume, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	if vol, ok := t.srv.volIndex[volume]; ok {
		return vol, nil
	}
	return nil, errors.New("temp: no such volume exists")
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
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	return nil
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

func (t *Client) GetINodeIndex(volume agro.VolumeID) (agro.INodeID, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	return t.srv.inode[volume], nil
}

func (t *Client) GetINodeIndexes() (map[agro.VolumeID]agro.INodeID, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	out := make(map[agro.VolumeID]agro.INodeID)
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

func (t *Client) LockData() {
	t.srv.mut.Lock()
}

func (t *Client) UnlockData() {
	t.srv.mut.Unlock()
}

func (t *Client) GetData(x string) (interface{}, bool) {
	out, exists := t.srv.keys[x]
	return out, exists
}

func (t *Client) SetData(x string, v interface{}) {
	t.srv.keys[x] = v
}
