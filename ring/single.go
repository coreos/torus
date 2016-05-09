package ring

import (
	"fmt"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

type single struct {
	version     int
	peer        *models.PeerInfo
	permutation agro.PeerPermutation
}

func init() {
	registerRing(Single, "single", makeSingle)
}

func makeSingle(r *models.Ring) (agro.Ring, error) {
	if len(r.Peers) != 1 {
		return nil, agro.ErrInvalid
	}
	return &single{
		version: int(r.Version),
		peer:    r.Peers[0],
		permutation: agro.PeerPermutation{
			Peers:       []string{r.Peers[0].UUID},
			Replication: 1,
		},
	}, nil
}

func (s *single) GetPeers(key agro.BlockRef) (agro.PeerPermutation, error) {
	return s.permutation, nil
}

func (s *single) Members() agro.PeerList { return []string{s.peer.UUID} }

func (s *single) Describe() string {
	return fmt.Sprintf("Ring: Single\nUUID: %s", s.peer.UUID)
}
func (s *single) Type() agro.RingType { return Single }
func (s *single) Version() int        { return s.version }

func (s *single) Marshal() ([]byte, error) {
	var out models.Ring

	out.Version = uint32(s.version)
	out.Type = uint32(s.Type())
	out.Peers = []*models.PeerInfo{s.peer}
	return out.Marshal()
}
