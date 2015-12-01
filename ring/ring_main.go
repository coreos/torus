package ring

import (
	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
)

const (
	Empty agro.RingType = iota
	Single
	Mod
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

func registerRing(t agro.RingType, newFunc createRingFunc) {
	if ringRegistry == nil {
		ringRegistry = make(map[agro.RingType]createRingFunc)
	}

	if _, ok := ringRegistry[t]; ok {
		panic("agro: attempted to register ring type " + string(t) + " twice")
	}

	ringRegistry[t] = newFunc
}

func CreateRing(r *models.Ring) (agro.Ring, error) {
	return ringRegistry[agro.RingType(r.Type)](r)
}
