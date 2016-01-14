package ring

import (
	"errors"
	"fmt"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

type unionRing struct {
	oldRing agro.Ring
	newRing agro.Ring
}

func init() {
	registerRing(Union, makeUnion)
}

func makeUnion(r *models.Ring) (agro.Ring, error) {
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

func NewUnionRing(oldRing agro.Ring, newRing agro.Ring) agro.Ring {
	return &unionRing{
		oldRing: oldRing,
		newRing: newRing,
	}
}

func (u *unionRing) GetBlockPeers(key agro.BlockRef) ([]string, error) {
	n, err := u.newRing.GetBlockPeers(key)
	if err != nil {
		return nil, err
	}
	o, err := u.oldRing.GetBlockPeers(key)
	if err != nil {
		return nil, err
	}
	return memberUnion(o, n), nil
}

func (u *unionRing) GetINodePeers(key agro.INodeRef) ([]string, error) {
	a, err := u.newRing.GetINodePeers(key)
	if err != nil {
		return nil, err
	}
	b, err := u.oldRing.GetINodePeers(key)
	if err != nil {
		return nil, err
	}
	return memberUnion(a, b), nil
}

func memberUnion(a []string, b []string) []string {
	out := append([]string(nil), a...)
	for _, m := range b {
		exists := false
		for _, x := range out {
			if x == m {
				exists = true
				break
			}
		}
		if !exists {
			out = append(out, m)
		}
	}
	return out
}

func (u *unionRing) Members() []string {
	return memberUnion(u.newRing.Members(), u.oldRing.Members())
}

func (u *unionRing) Describe() string {
	return fmt.Sprintf(
		"Union Ring:\nOld:\n%s\nNew:\n%s",
		u.oldRing.Describe(),
		u.newRing.Describe(),
	)
}
func (u *unionRing) Type() agro.RingType {
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
