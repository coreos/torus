package ring

import (
	"fmt"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

type single struct {
	version int
	uuid    string
}

func init() {
	registerRing(Single, makeSingle)
}

func makeSingle(r *models.Ring) (agro.Ring, error) {
	if len(r.UUIDs) != 1 {
		return nil, agro.ErrInvalid
	}
	return &single{
		version: int(r.Version),
		uuid:    r.UUIDs[0],
	}, nil
}

func (s *single) GetBlockPeers(key agro.BlockRef) ([]string, error) {
	return []string{s.uuid}, nil
}

func (s *single) GetINodePeers(key agro.INodeRef) ([]string, error) {
	return []string{s.uuid}, nil
}

func (s *single) Members() []string { return []string{s.uuid} }

func (s *single) Describe() string {
	return fmt.Sprintf("Ring: Single\nUUID: %s", s.uuid)
}
func (s *single) Type() agro.RingType { return Single }
func (s *single) Version() int        { return s.version }

func (s *single) Marshal() ([]byte, error) {
	var out models.Ring

	out.Version = uint32(s.version)
	out.Type = uint32(s.Type())
	out.UUIDs = []string{s.uuid}
	return out.Marshal()
}
