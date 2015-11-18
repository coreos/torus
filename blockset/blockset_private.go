package blockset

import "github.com/barakmich/agro"

type blockset interface {
	agro.Blockset
	makeID(agro.INodeRef) agro.BlockID
	setStore(store agro.BlockStore)
}

type BlockLayer int

// Constants for each type of layer, for serializing/deserializing
const (
	Basic BlockLayer = iota
	CRC
)

func CreateBlockset(kind BlockLayer, data []byte, store agro.BlockStore) (agro.Blockset, error) {
	panic("not implemented")
}
