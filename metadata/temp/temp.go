package temp

import (
	"errors"
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/coreos/torus"
	"github.com/coreos/torus/blockset"
	"github.com/coreos/torus/metadata"
	"github.com/coreos/torus/models"
	"github.com/coreos/torus/ring"
)

func init() {
	torus.RegisterMetadataService("temp", NewTemp)
}

type Server struct {
	mut sync.RWMutex

	inode map[torus.VolumeID]torus.INodeID
	vol   torus.VolumeID

	volIndex map[string]*models.Volume
	global   torus.GlobalMetadata
	peers    torus.PeerInfoList
	ring     torus.Ring
	newRing  torus.Ring

	keys map[string]interface{}

	ringListeners []chan torus.Ring
}

type Client struct {
	cfg    torus.Config
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
		global: torus.GlobalMetadata{
			BlockSize:        256,
			DefaultBlockSpec: blockset.MustParseBlockLayerSpec("crc,base"),
			INodeReplication: 2,
		},
		ring:  r,
		keys:  make(map[string]interface{}),
		inode: make(map[torus.VolumeID]torus.INodeID),
	}
}

func NewClient(cfg torus.Config, srv *Server) *Client {
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

func NewTemp(cfg torus.Config) (torus.MetadataService, error) {
	return NewClient(cfg, NewServer()), nil
}

func (t *Client) Kind() torus.MetadataKind {
	return torus.TempMetadata
}

func (t *Client) GlobalMetadata() torus.GlobalMetadata {
	return t.srv.global
}

func (t *Client) UUID() string {
	return t.uuid
}

func (t *Client) GetLease() (int64, error)        { return 1, nil }
func (t *Client) RenewLease(lease int64) error    { return nil }
func (t *Client) GetLockStatus(vid uint64) string { return "" }

func (t *Client) GetPeers() (torus.PeerInfoList, error) {
	t.srv.mut.RLock()
	defer t.srv.mut.RUnlock()
	peers := make(torus.PeerInfoList, len(t.srv.peers))
	copy(peers, t.srv.peers)
	return peers, nil
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

func (t *Client) NewVolumeID() (torus.VolumeID, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	t.srv.vol++
	return t.srv.vol, nil
}

func (t *Client) CommitINodeIndex(vol torus.VolumeID) (torus.INodeID, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	t.srv.inode[vol]++
	return t.srv.inode[vol], nil
}

func (t *Client) GetVolumes() ([]*models.Volume, torus.VolumeID, error) {
	t.srv.mut.RLock()
	defer t.srv.mut.RUnlock()

	var out []*models.Volume

	for _, v := range t.srv.volIndex {
		out = append(out, v)
	}
	return out, t.srv.vol, nil
}

func (t *Client) CreateVolume(volume *models.Volume) error {
	t.srv.volIndex[volume.Name] = volume
	t.srv.inode[torus.VolumeID(volume.Id)] = 1
	return nil
}

func (t *Client) GetVolume(volume string) (*models.Volume, error) {
	t.srv.mut.RLock()
	defer t.srv.mut.RUnlock()

	if vol, ok := t.srv.volIndex[volume]; ok {
		return vol, nil
	}
	return nil, errors.New(fmt.Sprintf("temp: volume %q not found", volume))
}

func (t *Client) GetRing() (torus.Ring, error) {
	t.srv.mut.RLock()
	defer t.srv.mut.RUnlock()
	return t.srv.ring, nil
}

func (t *Client) SubscribeNewRings(ch chan torus.Ring) {
	t.srv.SubscribeNewRings(ch)
}

func (t *Client) UnsubscribeNewRings(ch chan torus.Ring) {
	t.srv.UnsubscribeNewRings(ch)
}

func (s *Server) SubscribeNewRings(ch chan torus.Ring) {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.ringListeners = append(s.ringListeners, ch)
}

func (s *Server) UnsubscribeNewRings(ch chan torus.Ring) {
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

func (t *Client) SetRing(ring torus.Ring) error {
	return t.srv.SetRing(ring)
}

func (s *Server) SetRing(ring torus.Ring) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	if ring.Version()-1 != s.ring.Version() {
		return torus.ErrNonSequentialRing
	}
	s.ring = ring
	for _, c := range s.ringListeners {
		c <- s.ring
	}
	return nil
}

func (t *Client) GetINodeIndex(volume torus.VolumeID) (torus.INodeID, error) {
	t.srv.mut.RLock()
	defer t.srv.mut.RUnlock()
	return t.srv.inode[volume], nil
}

func (t *Client) GetINodeIndexes() (map[torus.VolumeID]torus.INodeID, error) {
	t.srv.mut.RLock()
	defer t.srv.mut.RUnlock()
	out := make(map[torus.VolumeID]torus.INodeID)
	for k, v := range t.srv.inode {
		out[k] = v
	}
	return out, nil
}

func (s *Server) Close() error {
	return nil
}

func (t *Client) WithContext(_ context.Context) torus.MetadataService {
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

func (t *Client) DeleteVolume(name string) error {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	delete(t.srv.keys, name)
	delete(t.srv.volIndex, name)
	return nil
}
