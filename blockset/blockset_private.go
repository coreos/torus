package blockset

import "github.com/barakmich/agro"

type blockset interface {
	agro.Blockset
	makeID(agro.INodeRef) agro.BlockID
	setStore(store agro.BlockStore)
}

type BlockLayer int

const (
	Basic BlockLayer = iota
)

func CreateBlockset(kind BlockLayer, data []byte, store agro.BlockStore) (agro.Blockset, error) {
	panic("not implemented")
}
