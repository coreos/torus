package ring

import (
	"fmt"
	"hash/crc32"
	"reflect"
	"sort"

	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
)

type mod struct {
	version  int
	rep      int
	peers    torus.PeerInfoList
	peerlist []string
	npeers   int
}

func init() {
	registerRing(Mod, "mod", makeMod)
}

func makeMod(r *models.Ring) (torus.Ring, error) {
	rep := int(r.ReplicationFactor)
	if rep == 0 {
		rep = 1
	}
	pil := torus.PeerInfoList(r.Peers)
	return &mod{
		version:  int(r.Version),
		peers:    pil,
		peerlist: sort.StringSlice([]string(pil.PeerList())),
		rep:      rep,
	}, nil
}

func (m *mod) GetPeers(key torus.BlockRef) (torus.PeerPermutation, error) {
	permute := make([]string, len(m.peerlist))
	crc := crc32.ChecksumIEEE(key.ToBytes())
	sum := int(crc) % len(m.peers)
	copy(permute, m.peerlist[sum:])
	copy(permute[len(m.peerlist)-sum:], m.peerlist[:sum])
	return torus.PeerPermutation{
		Peers:       permute,
		Replication: m.rep,
	}, nil
}

func (m *mod) Members() torus.PeerList { return m.peers.PeerList() }

func (m *mod) Describe() string {
	s := fmt.Sprintf("Ring: Mod\nReplication:%d\nPeers:", m.rep)
	for _, x := range m.peerlist {
		s += fmt.Sprintf("\n\t%s", x)
	}
	return s
}
func (m *mod) Type() torus.RingType { return Mod }
func (m *mod) Version() int        { return m.version }

func (m *mod) Marshal() ([]byte, error) {
	var out models.Ring

	out.Version = uint32(m.version)
	out.ReplicationFactor = uint32(m.rep)
	out.Type = uint32(m.Type())
	out.Peers = m.peers
	return out.Marshal()
}

func (m *mod) AddPeers(peers torus.PeerInfoList) (torus.Ring, error) {
	newPeers := m.peers.Union(peers)
	if reflect.DeepEqual(newPeers.PeerList(), m.peers.PeerList()) {
		return nil, torus.ErrExists
	}
	newm := &mod{
		version:  m.version + 1,
		rep:      m.rep,
		peers:    newPeers,
		peerlist: sort.StringSlice([]string(newPeers.PeerList())),
		npeers:   len(newPeers),
	}
	return newm, nil
}

func (m *mod) RemovePeers(pl torus.PeerList) (torus.Ring, error) {
	newPeers := m.peers.AndNot(pl)
	if len(newPeers) == len(m.peers) {
		return nil, torus.ErrNotExist
	}

	newm := &mod{
		version:  m.version + 1,
		rep:      m.rep,
		peers:    newPeers,
		peerlist: sort.StringSlice([]string(newPeers.PeerList())),
		npeers:   len(newPeers),
	}
	return newm, nil
}

func (m *mod) ChangeReplication(r int) (torus.Ring, error) {
	newm := &mod{
		version:  m.version + 1,
		rep:      r,
		peers:    m.peers,
		peerlist: sort.StringSlice([]string(m.peers.PeerList())),
		npeers:   len(m.peers),
	}
	return newm, nil
}
