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
	peers   []string
}

func init() {
	registerRing(Mod, makeMod)
}

func makeMod(r *models.Ring) (agro.Ring, error) {
	return &mod{
		version: int(r.Version),
		peers:   sort.StringSlice(r.UUIDs),
	}, nil
}

func (m *mod) GetBlockPeers(key agro.BlockRef, n int) ([]string, error) {
	var permute []string
	crc := crc32.ChecksumIEEE(key.ToBytes())
	sum := int(crc) % len(m.peers)
	permute = append(permute, m.peers[sum:]...)
	permute = append(permute, m.peers[:sum]...)
	return permute[:n], nil
}

func (m *mod) GetINodePeers(key agro.INodeRef, n int) ([]string, error) {
	return m.GetBlockPeers(agro.BlockRef{
		INodeRef: key,
		Index:    agro.IndexID(0),
	}, n)
}

func (m *mod) Members() []string { return m.peers }

func (m *mod) Describe() string {
	s := fmt.Sprintf("Ring: Mod\nPeers:")
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
	out.Type = uint32(m.Type())
	out.UUIDs = m.peers
	return out.Marshal()
}
