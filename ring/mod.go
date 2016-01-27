package ring

import (
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

type mod struct {
	version int
	rep     int
	peers   []string
	npeers  int
}

func init() {
	registerRing(Mod, makeMod)
}

func makeMod(r *models.Ring) (agro.Ring, error) {
	rep := int(r.ReplicationFactor)
	if rep == 0 {
		rep = 1
	}
	return &mod{
		version: int(r.Version),
		peers:   sort.StringSlice(r.UUIDs),
		rep:     rep,
		npeers:  len(r.UUIDs),
	}, nil
}

func (m *mod) GetPeers(key agro.BlockRef) (agro.PeerList, error) {
	permute := make([]string, m.rep)
	crc := crc32.ChecksumIEEE(key.ToBytes())
	sum := int(crc) % len(m.peers)
	copy(permute, m.peers[sum:])
	if m.npeers-sum >= m.rep {
		return permute, nil
	}
	copy(permute[m.npeers-sum:], m.peers)
	return permute, nil
}

func (m *mod) Members() agro.PeerList { return append([]string(nil), m.peers...) }

func (m *mod) Describe() string {
	s := fmt.Sprintf("Ring: Mod\nReplication:%d\nPeers:", m.rep)
	for _, x := range m.peers {
		s += fmt.Sprintf("\n\t%s", x)
	}
	return s
}
func (m *mod) Type() agro.RingType { return Mod }
func (m *mod) Version() int        { return m.version }

func (m *mod) Marshal() ([]byte, error) {
	var out models.Ring

	out.Version = uint32(m.version)
	out.ReplicationFactor = uint32(m.rep)
	out.Type = uint32(m.Type())
	out.UUIDs = m.peers
	return out.Marshal()
}
