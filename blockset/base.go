package blockset

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"

	"golang.org/x/net/context"

	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/agro"
)

type baseBlockset struct {
	ids    uint64
	blocks []agro.BlockRef
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
		blocks: make([]agro.BlockRef, 0),
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
	if b.blocks[i].IsZero() {
		return make([]byte, b.store.BlockSize()), nil
	}
	clog.Tracef("base: getting block at BlockID %s", b.blocks[i])
	bytes, err := b.store.GetBlock(ctx, b.blocks[i])
	if err != nil {
		promBaseFail.Inc()
		return nil, err
	}
	return bytes, err
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

func (b *baseBlockset) makeID(i agro.INodeRef) agro.BlockRef {
	id := atomic.AddUint64(&b.ids, 2)
	return agro.BlockRef{
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
		_, err := buf.Write(x.ToBytes())
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (b *baseBlockset) setStore(s agro.BlockStore) {
	b.store = s
}

func (b *baseBlockset) getStore() agro.BlockStore {
	return b.store
}

func (b *baseBlockset) Unmarshal(data []byte) error {
	r := bytes.NewReader(data)
	var l int32
	err := binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return err
	}
	out := make([]agro.BlockRef, l)
	for i := 0; i < int(l); i++ {
		buf := make([]byte, agro.BlockRefByteSize)
		_, err := r.Read(buf)
		if err != nil {
			return err
		}
		out[i] = agro.BlockRefFromBytes(buf)
	}
	b.blocks = out
	return nil
}

func (b *baseBlockset) GetSubBlockset() agro.Blockset { return nil }

func (b *baseBlockset) GetLiveINodes() *roaring.Bitmap {
	out := roaring.NewBitmap()
	for _, blk := range b.blocks {
		if blk.IsZero() {
			continue
		}
		out.Add(uint32(blk.INode))
	}
	return out
}

func (b *baseBlockset) Truncate(lastIndex int) error {
	if lastIndex <= len(b.blocks) {
		b.blocks = b.blocks[:lastIndex]
		return nil
	}
	toadd := lastIndex - len(b.blocks)
	for toadd != 0 {
		b.blocks = append(b.blocks, agro.ZeroBlock())
		toadd--
	}
	return nil
}
