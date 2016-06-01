package blockset

import (
	"sync/atomic"

	"golang.org/x/net/context"

	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/torus"
)

type baseBlockset struct {
	ids       uint64
	blocks    []torus.BlockRef
	store     torus.BlockStore
	blocksize uint64
}

var _ blockset = &baseBlockset{}

func init() {
	RegisterBlockset(Base, func(_ string, store torus.BlockStore, _ blockset) (blockset, error) {
		return newBaseBlockset(store), nil
	})
}

func newBaseBlockset(store torus.BlockStore) *baseBlockset {
	b := &baseBlockset{
		blocks: make([]torus.BlockRef, 0),
		store:  store,
	}
	if store != nil {
		b.blocksize = store.BlockSize()
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
		return nil, torus.ErrBlockNotExist
	}
	if b.blocks[i].IsZero() {
		return make([]byte, b.store.BlockSize()), nil
	}
	if torus.BlockLog.LevelAt(capnslog.TRACE) {
		torus.BlockLog.Tracef("base: getting block %d at BlockID %s", i, b.blocks[i])
	}
	bytes, err := b.store.GetBlock(ctx, b.blocks[i])
	if err != nil {
		promBaseFail.Inc()
		return nil, err
	}
	return bytes, err
}

func (b *baseBlockset) PutBlock(ctx context.Context, inode torus.INodeRef, i int, data []byte) error {
	if i > len(b.blocks) {
		return torus.ErrBlockNotExist
	}
	// if v, ok := ctx.Value("isEmpty").(bool); ok && v {
	// 	clog.Debug("copying empty block")
	// 	if i == len(b.blocks) {
	// 		b.blocks = append(b.blocks, torus.ZeroBlock())
	// 	} else {
	// 		b.blocks[i] = torus.ZeroBlock()
	// 	}
	// 	return nil
	// }
	newBlockID := b.makeID(inode)
	if torus.BlockLog.LevelAt(capnslog.TRACE) {
		torus.BlockLog.Tracef("base: writing block %d at BlockID %s", i, newBlockID)
	}
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

func (b *baseBlockset) makeID(i torus.INodeRef) torus.BlockRef {
	id := atomic.AddUint64(&b.ids, 1)
	return torus.BlockRef{
		INodeRef: i,
		Index:    torus.IndexID(id),
	}
}

func (b *baseBlockset) Marshal() ([]byte, error) {
	buf := make([]byte, len(b.blocks)*torus.BlockRefByteSize)
	for i, x := range b.blocks {
		x.ToBytesBuf(buf[(i * torus.BlockRefByteSize) : (i+1)*torus.BlockRefByteSize])
	}
	return buf, nil
}

func (b *baseBlockset) setStore(s torus.BlockStore) {
	b.blocksize = s.BlockSize()
	b.store = s
}

func (b *baseBlockset) getStore() torus.BlockStore {
	return b.store
}

func (b *baseBlockset) Unmarshal(data []byte) error {
	l := len(data) / torus.BlockRefByteSize
	out := make([]torus.BlockRef, l)
	for i := 0; i < l; i++ {
		out[i] = torus.BlockRefFromBytes(data[(i * torus.BlockRefByteSize) : (i+1)*torus.BlockRefByteSize])
	}
	b.blocks = out
	return nil
}

func (b *baseBlockset) GetSubBlockset() torus.Blockset { return nil }

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

func (b *baseBlockset) Truncate(lastIndex int, _ uint64) error {
	if lastIndex <= len(b.blocks) {
		b.blocks = b.blocks[:lastIndex]
		return nil
	}
	toadd := lastIndex - len(b.blocks)
	for toadd != 0 {
		b.blocks = append(b.blocks, torus.ZeroBlock())
		toadd--
	}
	return nil
}

func (b *baseBlockset) Trim(from, to int) error {
	if from >= len(b.blocks) {
		return nil
	}
	if to > len(b.blocks) {
		to = len(b.blocks)
	}
	for i := from; i < to; i++ {
		b.blocks[i] = torus.ZeroBlock()
	}
	return nil
}

func (b *baseBlockset) GetAllBlockRefs() []torus.BlockRef {
	out := make([]torus.BlockRef, len(b.blocks))
	copy(out, b.blocks)
	return out
}

func (b *baseBlockset) String() string {
	out := "[\n"
	for _, x := range b.blocks {
		out += x.String() + "\n"
	}
	out += "]"
	return out
}
