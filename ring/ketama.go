package ring

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"

	"github.com/serialx/hashring"
)

type ketama struct {
	version int
	rep     int
	peers   agro.PeerInfoList
	ring    *hashring.HashRing
}

func init() {
	registerRing(Ketama, "ketama", makeKetama)
}

func makeKetama(r *models.Ring) (agro.Ring, error) {
	rep := int(r.ReplicationFactor)
	if rep == 0 {
		rep = 1
	}
	pi := agro.PeerInfoList(r.Peers)
	return &ketama{
		version: int(r.Version),
		peers:   pi,
		rep:     rep,
		ring:    hashring.New(pi.PeerList()),
	}, nil
}

func (k *ketama) GetPeers(key agro.BlockRef) (agro.PeerPermutation, error) {
	s, ok := k.ring.GetNodes(string(key.ToBytes()), len(k.peers))
	if !ok {
		return agro.PeerPermutation{}, errors.New("couldn't get sufficient nodes")
	}
	return agro.PeerPermutation{
		Peers:       s,
		Replication: k.rep,
	}, nil
}

func (k *ketama) Members() agro.PeerList { return k.peers.PeerList() }

func (k *ketama) Describe() string {
	s := fmt.Sprintf("Ring: Ketama\nReplication:%d\nPeers:", k.rep)
	for _, x := range k.peers {
		s += fmt.Sprintf("\n\t%s", x)
	}
	return s
}
func (k *ketama) Type() agro.RingType { return Ketama }
func (k *ketama) Version() int        { return k.version }

func (k *ketama) Marshal() ([]byte, error) {
	var out models.Ring

	out.Version = uint32(k.version)
	out.ReplicationFactor = uint32(k.rep)
	out.Type = uint32(k.Type())
	out.Peers = k.peers
	return out.Marshal()
}

func (k *ketama) AddPeers(peers agro.PeerInfoList, mods ...agro.RingModification) (agro.Ring, error) {
	newPeers := k.peers.Union(peers)
	if reflect.DeepEqual(newPeers.PeerList(), k.peers.PeerList()) {
		return nil, errors.New("no difference in membership")
	}
	newk := &ketama{
		version: k.version + 1,
		rep:     k.rep,
		peers:   newPeers,
		ring:    hashring.New(newPeers.PeerList()),
	}
	for _, x := range mods {
		x.ModifyRing(newk)
	}
	return newk, nil
}

func (k *ketama) RemovePeers(pl agro.PeerList, mods ...agro.RingModification) (agro.Ring, error) {
	newPeers := k.peers.AndNot(pl)
	if len(newPeers) == len(k.Members()) {
		return nil, errors.New("no difference in membership")
	}

	newk := &ketama{
		version: k.version + 1,
		rep:     k.rep,
		peers:   newPeers,
		ring:    hashring.New(newPeers.PeerList()),
	}
	for _, x := range mods {
		x.ModifyRing(newk)
	}
	return newk, nil
}

func (k *ketama) ChangeReplication(r int) {
	k.rep = r
}
