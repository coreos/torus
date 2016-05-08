package block

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
)

var _ agro.BlockStore = &tempBlockStore{}

func init() {
	agro.RegisterBlockStore("temp", openTempBlockStore)
}

type tempBlockStore struct {
	mut       sync.RWMutex
	store     map[agro.BlockRef][]byte
	nBlocks   uint64
	name      string
	blockSize uint64
}

func openTempBlockStore(name string, cfg agro.Config, gmd agro.GlobalMetadata) (agro.BlockStore, error) {
	nBlocks := cfg.StorageSize / gmd.BlockSize
	promBlocksAvail.WithLabelValues(name).Set(float64(nBlocks))
	promBlocks.WithLabelValues(name).Set(0)
	promBytesPerBlock.Set(float64(gmd.BlockSize))
	return &tempBlockStore{
		store:     make(map[agro.BlockRef][]byte),
		nBlocks:   nBlocks,
		name:      name,
		blockSize: gmd.BlockSize,
	}, nil
}

func (t *tempBlockStore) Kind() string      { return "temp" }
func (t *tempBlockStore) Flush() error      { return nil }
func (t *tempBlockStore) BlockSize() uint64 { return t.blockSize }

func (t *tempBlockStore) Close() error {
	t.mut.Lock()
	defer t.mut.Unlock()
	if t.store != nil {
		t.store = nil
	}
	return nil
}

func (t *tempBlockStore) NumBlocks() uint64 {
	t.mut.Lock()
	defer t.mut.Unlock()
	return t.nBlocks
}

func (t *tempBlockStore) UsedBlocks() uint64 {
	t.mut.Lock()
	defer t.mut.Unlock()
	return uint64(len(t.store))
}

func (t *tempBlockStore) HasBlock(_ context.Context, s agro.BlockRef) (bool, error) {
	t.mut.Lock()
	defer t.mut.Unlock()
	_, ok := t.store[s]
	return ok, nil
}

func (t *tempBlockStore) GetBlock(_ context.Context, s agro.BlockRef) ([]byte, error) {
	t.mut.RLock()
	defer t.mut.RUnlock()

	if t.store == nil {
		promBlocksFailed.WithLabelValues(t.name).Inc()
		return nil, agro.ErrClosed
	}

	x, ok := t.store[s]
	if !ok {
		promBlocksFailed.WithLabelValues(t.name).Inc()
		return nil, agro.ErrBlockNotExist
	}
	promBlocksRetrieved.WithLabelValues(t.name).Inc()
	return x, nil
}

func (t *tempBlockStore) WriteBlock(_ context.Context, s agro.BlockRef, data []byte) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.store == nil {
		promBlockWritesFailed.WithLabelValues(t.name).Inc()
		return agro.ErrClosed
	}
	if int(t.nBlocks) <= len(t.store) {
		return agro.ErrOutOfSpace
	}
	buf := make([]byte, len(data))
	copy(buf, data)
	t.store[s] = buf
	promBlocks.WithLabelValues(t.name).Set(float64(len(t.store)))
	promBlocksWritten.WithLabelValues(t.name).Inc()
	return nil
}

func (t *tempBlockStore) WriteBuf(_ context.Context, s agro.BlockRef) ([]byte, error) {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.store == nil {
		promBlockWritesFailed.WithLabelValues(t.name).Inc()
		return nil, agro.ErrClosed
	}
	if int(t.nBlocks) <= len(t.store) {
		return nil, agro.ErrOutOfSpace
	}
	buf := make([]byte, t.blockSize)
	t.store[s] = buf
	promBlocks.WithLabelValues(t.name).Set(float64(len(t.store)))
	promBlocksWritten.WithLabelValues(t.name).Inc()
	return buf, nil
}

func (t *tempBlockStore) DeleteBlock(_ context.Context, s agro.BlockRef) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.store == nil {
		promBlockDeletesFailed.WithLabelValues(t.name).Inc()
		return agro.ErrClosed
	}

	delete(t.store, s)
	promBlocks.WithLabelValues(t.name).Set(float64(len(t.store)))
	promBlocksDeleted.WithLabelValues(t.name).Inc()
	return nil
}

func (t *tempBlockStore) BlockIterator() agro.BlockIterator {
	t.mut.RLock()
	defer t.mut.RUnlock()
	var blocks []agro.BlockRef
	for k := range t.store {
		blocks = append(blocks, k)
	}
	return &tempIterator{
		blocks: blocks,
		index:  -1,
	}
}

type tempIterator struct {
	blocks []agro.BlockRef
	index  int
}

func (i *tempIterator) Err() error { return nil }

func (i *tempIterator) Next() bool {
	i.index++
	if i.index >= len(i.blocks) {
		return false
	}
	return true
}

func (i *tempIterator) BlockRef() agro.BlockRef { return i.blocks[i.index] }

func (i *tempIterator) Close() error { return nil }
