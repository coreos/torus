package ring

import (
	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/torus", "ring")

const (
	Empty torus.RingType = iota
	Single
	Mod
	Union
	Ketama
)

func Unmarshal(b []byte) (torus.Ring, error) {
	var a models.Ring
	err := a.Unmarshal(b)
	if err != nil {
		return nil, err
	}
	return CreateRing(&a)
}

type createRingFunc func(r *models.Ring) (torus.Ring, error)

var ringRegistry map[torus.RingType]createRingFunc
var ringNames map[string]torus.RingType

func registerRing(t torus.RingType, name string, newFunc createRingFunc) {
	if ringRegistry == nil {
		ringRegistry = make(map[torus.RingType]createRingFunc)
	}

	if _, ok := ringRegistry[t]; ok {
		panic("torus: attempted to register ring type " + string(t) + " twice")
	}

	ringRegistry[t] = newFunc

	if ringNames == nil {
		ringNames = make(map[string]torus.RingType)
	}

	if _, ok := ringNames[name]; ok {
		panic("torus: attempted to register ring name " + name + " twice")
	}

	ringNames[name] = t
}

func CreateRing(r *models.Ring) (torus.Ring, error) {
	return ringRegistry[torus.RingType(r.Type)](r)
}

func RingTypeFromString(s string) (torus.RingType, bool) {
	v, ok := ringNames[s]
	return v, ok
}
