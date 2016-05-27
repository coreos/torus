package ring

import (
	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
)

type empty struct {
	version int
}

func init() {
	registerRing(Empty, "empty", makeEmpty)
}

func makeEmpty(r *models.Ring) (torus.Ring, error) {
	return &empty{
		version: int(r.Version),
	}, nil
}

func (e *empty) GetPeers(key torus.BlockRef) (torus.PeerPermutation, error) {
	return torus.PeerPermutation{
		Peers:       []string{},
		Replication: 0,
	}, nil
}

func (e *empty) Members() torus.PeerList { return []string{} }

func (e *empty) Describe() string {
	return "Ring: Empty"
}
func (e *empty) Type() torus.RingType { return Empty }
func (e *empty) Version() int        { return e.version }

func (e *empty) Marshal() ([]byte, error) {
	var out models.Ring

	out.Version = uint32(e.version)
	out.Type = uint32(e.Type())
	return out.Marshal()
}
