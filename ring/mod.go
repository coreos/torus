package ring

import (
	"errors"
	"fmt"
	"hash/crc32"
	"reflect"
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
	registerRing(Mod, "mod", makeMod)
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

func (m *mod) AddPeers(pl agro.PeerList, mods ...agro.RingModification) (agro.Ring, error) {
	newPeers := sort.StringSlice(m.Members().Union(pl))
	if reflect.DeepEqual(newPeers, m.Members()) {
		return nil, errors.New("no difference in membership")
	}
	newm := &mod{
		version: m.version + 1,
		rep:     m.rep,
		peers:   newPeers,
		npeers:  len(newPeers),
	}
	for _, x := range mods {
		x.ModifyRing(newm)
	}
	return newm, nil
}

func (m *mod) RemovePeers(pl agro.PeerList, mods ...agro.RingModification) (agro.Ring, error) {
	newPeers := sort.StringSlice(m.Members().AndNot(pl))
	if reflect.DeepEqual(newPeers, m.Members()) {
		return nil, errors.New("no difference in membership")
	}
	newm := &mod{
		version: m.version + 1,
		rep:     m.rep,
		peers:   newPeers,
		npeers:  len(newPeers),
	}
	for _, x := range mods {
		x.ModifyRing(newm)
	}
	return newm, nil
}

func (m *mod) ChangeReplication(r int) {
	m.rep = r
}
