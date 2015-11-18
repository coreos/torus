package blockset

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"

	"github.com/barakmich/agro"
)

type basicBlockset struct {
	ids    int64
	blocks []agro.BlockID
	store  agro.BlockStore
}

var _ blockset = &basicBlockset{}

func newBasicBlockset(store agro.BlockStore) *basicBlockset {
	b := &basicBlockset{
		blocks: make([]agro.BlockID, 0),
		store:  store,
	}
	return b
}

func (b *basicBlockset) Length() int {
	return len(b.blocks)
}

func (b *basicBlockset) GetBlock(i int) ([]byte, error) {
	if i >= len(b.blocks) {
		return nil, agro.ErrBlockNotExist
	}
	return b.store.GetBlock(b.blocks[i])
}

func (b *basicBlockset) PutBlock(inode agro.INodeRef, i int, data []byte) error {
	if i > len(b.blocks) {
		return agro.ErrBlockNotExist
	}
	newBlockID := b.makeID(inode)
	err := b.store.WriteBlock(newBlockID, data)
	if err != nil {
		return err
	}
	if i == len(b.blocks) {
		b.blocks = append(b.blocks, newBlockID)
	} else {
		b.blocks[i] = newBlockID
	}
	return nil
}

func (b *basicBlockset) makeID(i agro.INodeRef) agro.BlockID {
	id := atomic.AddInt64(&b.ids, 2)
	return agro.BlockID{
		INodeRef: i,
		Index:    id,
	}
}

func (b *basicBlockset) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, int32(len(b.blocks)))
	if err != nil {
		return nil, err
	}
	for _, x := range b.blocks {
		err := binary.Write(buf, binary.LittleEndian, x)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (b *basicBlockset) setStore(s agro.BlockStore) {
	b.store = s
}

func (b *basicBlockset) Unmarshal(data []byte) error {
	r := bytes.NewReader(data)
	var l int32
	err := binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return err
	}
	out := make([]agro.BlockID, l)
	err = binary.Read(r, binary.LittleEndian, &out)
	if err != nil {
		return err
	}
	b.blocks = out
	return nil
}

func (b *basicBlockset) GetSubBlockset() agro.Blockset { return nil }
