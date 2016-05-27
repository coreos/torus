package ring

import (
	"errors"
	"fmt"

	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
)

type unionRing struct {
	oldRing torus.Ring
	newRing torus.Ring
}

func init() {
	registerRing(Union, "union", makeUnion)
}

func makeUnion(r *models.Ring) (torus.Ring, error) {
	var err error
	out := &unionRing{}
	oldb, ok := r.Attrs["old"]
	if !ok {
		return nil, errors.New("no old ring in union ring data")
	}
	out.oldRing, err = Unmarshal(oldb)
	if err != nil {
		return nil, err
	}
	newb, ok := r.Attrs["new"]
	if !ok {
		return nil, errors.New("no new ring in union ring data")
	}
	out.newRing, err = Unmarshal(newb)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func NewUnionRing(oldRing torus.Ring, newRing torus.Ring) torus.Ring {
	return &unionRing{
		oldRing: oldRing,
		newRing: newRing,
	}
}

func (u *unionRing) GetPeers(key torus.BlockRef) (torus.PeerPermutation, error) {
	n, err := u.newRing.GetPeers(key)
	if err != nil {
		return torus.PeerPermutation{}, err
	}
	o, err := u.oldRing.GetPeers(key)
	if err != nil {
		return torus.PeerPermutation{}, err
	}
	return torus.PeerPermutation{
		Peers:       o.Peers.Union(n.Peers),
		Replication: n.Replication,
	}, nil
}

func (u *unionRing) Members() torus.PeerList {
	return u.newRing.Members().Union(u.oldRing.Members())
}

func (u *unionRing) Describe() string {
	return fmt.Sprintf(
		"Union Ring:\nOld:\n%s\nNew:\n%s",
		u.oldRing.Describe(),
		u.newRing.Describe(),
	)
}
func (u *unionRing) Type() torus.RingType {
	return Union
}
func (u *unionRing) Version() int {
	return u.newRing.Version()
}

func (u *unionRing) Marshal() ([]byte, error) {
	var out models.Ring

	out.Version = uint32(u.Version())
	out.Type = uint32(u.Type())
	out.Attrs = make(map[string][]byte)
	b, err := u.oldRing.Marshal()
	if err != nil {
		return nil, err
	}
	out.Attrs["old"] = b
	b, err = u.newRing.Marshal()
	if err != nil {
		return nil, err
	}
	out.Attrs["new"] = b
	return out.Marshal()
}
