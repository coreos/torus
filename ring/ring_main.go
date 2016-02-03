package ring

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

const (
	Empty agro.RingType = iota
	Single
	Mod
	Union
	Ketama
)

func Unmarshal(b []byte) (agro.Ring, error) {
	var a models.Ring
	err := a.Unmarshal(b)
	if err != nil {
		return nil, err
	}
	return CreateRing(&a)
}

type createRingFunc func(r *models.Ring) (agro.Ring, error)

var ringRegistry map[agro.RingType]createRingFunc
var ringNames map[string]agro.RingType

func registerRing(t agro.RingType, name string, newFunc createRingFunc) {
	if ringRegistry == nil {
		ringRegistry = make(map[agro.RingType]createRingFunc)
	}

	if _, ok := ringRegistry[t]; ok {
		panic("agro: attempted to register ring type " + string(t) + " twice")
	}

	ringRegistry[t] = newFunc

	if ringNames == nil {
		ringNames = make(map[string]agro.RingType)
	}

	if _, ok := ringNames[name]; ok {
		panic("agro: attempted to register ring name " + name + " twice")
	}

	ringNames[name] = t
}

func CreateRing(r *models.Ring) (agro.Ring, error) {
	return ringRegistry[agro.RingType(r.Type)](r)
}

func RingTypeFromString(s string) (agro.RingType, bool) {
	v, ok := ringNames[s]
	return v, ok
}

type changeReplication struct{ to int }

func (c changeReplication) ModifyRing(r agro.ModifyableRing) {
	r.ChangeReplication(c.to)
}

func ReplicationLevel(x int) agro.RingModification {
	return changeReplication{to: x}
}
