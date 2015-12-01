package ring

import (
	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
)

type empty struct {
	version int
}

func init() {
	registerRing(Empty, makeEmpty)
}

func makeEmpty(r *models.Ring) (agro.Ring, error) {
	return &empty{
		version: int(r.Version),
	}, nil
}

func (e *empty) GetBlockPeers(key agro.BlockRef, n int) ([]string, error) {
	return []string{}, nil
}

func (e *empty) GetINodePeers(key agro.INodeRef, n int) ([]string, error) {
	return []string{}, nil
}

func (e *empty) Members() []string { return []string{} }

func (e *empty) Describe() string {
	return "Ring: Empty"
}
func (e *empty) Type() agro.RingType { return Empty }
func (e *empty) Version() int        { return e.version }

func (e *empty) Marshal() ([]byte, error) {
	var out models.Ring

	out.Version = uint32(e.version)
	out.Type = uint32(e.Type())
	return out.Marshal()
}
