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
		ring:    hashring.NewWithWeights(pi.GetWeights()),
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

func (k *ketama) AddPeers(peers agro.PeerInfoList) (agro.Ring, error) {
	newPeers := k.peers.Union(peers)
	if reflect.DeepEqual(newPeers.PeerList(), k.peers.PeerList()) {
		return nil, agro.ErrExists
	}
	newk := &ketama{
		version: k.version + 1,
		rep:     k.rep,
		peers:   newPeers,
		ring:    hashring.NewWithWeights(newPeers.GetWeights()),
	}
	return newk, nil
}

func (k *ketama) RemovePeers(pl agro.PeerList) (agro.Ring, error) {
	newPeers := k.peers.AndNot(pl)
	if len(newPeers) == len(k.Members()) {
		return nil, agro.ErrNotExist
	}

	newk := &ketama{
		version: k.version + 1,
		rep:     k.rep,
		peers:   newPeers,
		ring:    hashring.NewWithWeights(newPeers.GetWeights()),
	}
	return newk, nil
}

func (k *ketama) ChangeReplication(r int) (agro.Ring, error) {
	newk := &ketama{
		version: k.version + 1,
		rep:     r,
		peers:   k.peers,
		ring:    k.ring,
	}
	return newk, nil
}
