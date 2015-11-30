package blockset

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"

	"golang.org/x/net/context"

	"github.com/barakmich/agro"
)

type baseBlockset struct {
	ids    uint64
	blocks []agro.BlockID
	store  agro.BlockStore
}

var _ blockset = &baseBlockset{}

func init() {
	RegisterBlockset(Base, func(_ string, store agro.BlockStore, _ blockset) (blockset, error) {
		return newBaseBlockset(store), nil
	})
}

func newBaseBlockset(store agro.BlockStore) *baseBlockset {
	b := &baseBlockset{
		blocks: make([]agro.BlockID, 0),
		store:  store,
	}
	return b
}

func (b *baseBlockset) Length() int {
	return len(b.blocks)
}

func (b *baseBlockset) Kind() uint32 {
	return uint32(Base)
}

func (b *baseBlockset) GetBlock(ctx context.Context, i int) ([]byte, error) {
	if i >= len(b.blocks) {
		return nil, agro.ErrBlockNotExist
	}
	clog.Tracef("base: getting block at BlockID %s", b.blocks[i])
	return b.store.GetBlock(ctx, b.blocks[i])
}

func (b *baseBlockset) PutBlock(ctx context.Context, inode agro.INodeRef, i int, data []byte) error {
	if i > len(b.blocks) {
		return agro.ErrBlockNotExist
	}
	newBlockID := b.makeID(inode)
	err := b.store.WriteBlock(ctx, newBlockID, data)
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

func (b *baseBlockset) makeID(i agro.INodeRef) agro.BlockID {
	id := atomic.AddUint64(&b.ids, 2)
	return agro.BlockID{
		INodeRef: i,
		Index:    agro.IndexID(id),
	}
}

func (b *baseBlockset) Marshal() ([]byte, error) {
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

func (b *baseBlockset) setStore(s agro.BlockStore) {
	b.store = s
}

func (b *baseBlockset) Unmarshal(data []byte) error {
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

func (b *baseBlockset) GetSubBlockset() agro.Blockset { return nil }
