package blockset

import (
	"errors"
	"fmt"
	"strings"

	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
	"github.com/coreos/pkg/capnslog"
	"github.com/prometheus/client_golang/prometheus"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/torus", "blockset")

var (
	promCRCFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "torus_blockset_crc_failed_blocks",
		Help: "Number of blocks that failed due to CRC mismatch",
	})
	promBaseFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "torus_blockset_base_failed_blocks",
		Help: "Number of blocks that failed",
	})
)

func init() {
	prometheus.MustRegister(promCRCFail)
	prometheus.MustRegister(promBaseFail)
}

type blockset interface {
	torus.Blockset
	makeID(torus.INodeRef) torus.BlockRef
	setStore(store torus.BlockStore)
	getStore() torus.BlockStore
}

// Constants for each type of layer, for serializing/deserializing
const (
	Base torus.BlockLayerKind = iota
	CRC
	Replication
)

// CreateBlocksetFunc is the signature of a constructor used to create
// a BlockLayer.
type CreateBlocksetFunc func(opts string, store torus.BlockStore, subLayer blockset) (blockset, error)

var blocklayerRegistry map[torus.BlockLayerKind]CreateBlocksetFunc

// RegisterBlockset is the hook used for implementations of
// blocksets to register themselves to the system. This is usually
// called in the init() of the package that implements the blockset.
func RegisterBlockset(b torus.BlockLayerKind, newFunc CreateBlocksetFunc) {
	if blocklayerRegistry == nil {
		blocklayerRegistry = make(map[torus.BlockLayerKind]CreateBlocksetFunc)
	}

	if _, ok := blocklayerRegistry[b]; ok {
		panic("torus: attempted to register BlockLayer " + string(b) + " twice")
	}

	blocklayerRegistry[b] = newFunc
}

// CreateBlockset creates a Blockset of type b, with serialized data, backing store, and subLayer, if any)
// with the provided address.
func CreateBlockset(b torus.BlockLayer, store torus.BlockStore, subLayer blockset) (torus.Blockset, error) {
	return createBlockset(b, store, subLayer)
}
func createBlockset(b torus.BlockLayer, store torus.BlockStore, subLayer blockset) (blockset, error) {
	return blocklayerRegistry[b.Kind](b.Options, store, subLayer)
}

func UnmarshalFromProto(layers []*models.BlockLayer, store torus.BlockStore) (torus.Blockset, error) {
	l := len(layers)
	var layer blockset
	if l == 0 {
		return nil, errors.New("No layers to unmarshal")
	}
	for i := l - 1; i >= 0; i-- {
		m := layers[i]
		// Options must be stored by the blockset when serialized
		newl, err := createBlockset(torus.BlockLayer{Kind: torus.BlockLayerKind(m.Type), Options: ""}, store, layer)
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

func CreateBlocksetFromSpec(spec torus.BlockLayerSpec, store torus.BlockStore) (torus.Blockset, error) {
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

func ParseBlockLayerKind(s string) (torus.BlockLayerKind, error) {
	smalls := strings.ToLower(s)
	switch smalls {
	case "base":
		return Base, nil
	case "crc":
		return CRC, nil
	case "rep", "r":
		return Replication, nil
	default:
		return torus.BlockLayerKind(-1), fmt.Errorf("no such block layer type: %s", s)
	}
}

func ParseBlockLayerSpec(s string) (torus.BlockLayerSpec, error) {
	var out torus.BlockLayerSpec
	ss := strings.Split(s, ",")
	for _, x := range ss {
		opts := strings.Split(x, "=")
		k, err := ParseBlockLayerKind(opts[0])
		if err != nil {
			return nil, err
		}
		var opt string
		if len(opts) > 1 {
			opt = opts[1]
		}
		out = append(out, torus.BlockLayer{
			Kind:    k,
			Options: opt,
		})
	}
	return out, nil
}

func MustParseBlockLayerSpec(s string) torus.BlockLayerSpec {
	out, err := ParseBlockLayerSpec(s)
	if err != nil {
		panic(err)
	}
	return out
}
