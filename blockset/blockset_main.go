package blockset

import (
	"errors"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
	"github.com/coreos/pkg/capnslog"
)

var clog = capnslog.NewPackageLogger("github.com/barakmich/agro", "blockset")

type blockset interface {
	agro.Blockset
	makeID(agro.INodeRef) agro.BlockID
	setStore(store agro.BlockStore)
}

// Constants for each type of layer, for serializing/deserializing
const (
	Base agro.BlockLayer = iota
	CRC
)

// CreateBlocksetFunc is the signature of a constructor used to create
// a BlockLayer.
type CreateBlocksetFunc func(store agro.BlockStore, subLayer blockset) (blockset, error)

var blocklayerRegistry map[agro.BlockLayer]CreateBlocksetFunc

// RegisterBlockset is the hook used for implementions of
// blocksets to register themselves to the system. This is usually
// called in the init() of the package that implements the blockset.
func RegisterBlockset(b agro.BlockLayer, newFunc CreateBlocksetFunc) {
	if blocklayerRegistry == nil {
		blocklayerRegistry = make(map[agro.BlockLayer]CreateBlocksetFunc)
	}

	if _, ok := blocklayerRegistry[b]; ok {
		panic("agro: attempted to register BlockLayer " + string(b) + " twice")
	}

	blocklayerRegistry[b] = newFunc
}

// CreateBlockset creates a Blockset of type b, with serialized data, backing store, and subLayer, if any)
// with the provided address.
func CreateBlockset(b agro.BlockLayer, store agro.BlockStore, subLayer blockset) (agro.Blockset, error) {
	return createBlockset(b, store, subLayer)
}
func createBlockset(b agro.BlockLayer, store agro.BlockStore, subLayer blockset) (blockset, error) {
	return blocklayerRegistry[b](store, subLayer)
}

func MarshalToProto(bs agro.Blockset) ([]*models.BlockLayer, error) {
	var out []*models.BlockLayer
	var layer agro.Blockset
	for layer = bs; layer != nil; layer = layer.GetSubBlockset() {
		m, err := layer.Marshal()
		if err != nil {
			return nil, err
		}
		out = append(out, &models.BlockLayer{
			Type:    layer.Kind(),
			Content: m,
		})
	}
	return out, nil
}

func UnmarshalFromProto(layers []*models.BlockLayer, store agro.BlockStore) (agro.Blockset, error) {
	l := len(layers)
	var layer blockset
	if l == 0 {
		return nil, errors.New("No layers to unmarshal")
	}
	for i := l - 1; i >= 0; i-- {
		m := layers[i]
		newl, err := createBlockset(agro.BlockLayer(m.Type), store, layer)
		if err != nil {
			return nil, err
		}
		err = newl.Unmarshal(m.Content)
		if err != nil {
			return nil, err
		}
		layer = newl
	}
	return layer, nil
}

func CreateBlocksetFromSpec(spec agro.BlockLayerSpec, store agro.BlockStore) (agro.Blockset, error) {
	l := len(spec)
	var layer blockset
	if l == 0 {
		return nil, errors.New("Empty spec")
	}
	for i := l - 1; i >= 0; i-- {
		m := spec[i]
		newl, err := createBlockset(m, store, layer)
		if err != nil {
			return nil, err
		}
		layer = newl
	}
	return layer, nil
}
