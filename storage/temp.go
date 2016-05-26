package storage

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/coreos/torus"
)

var _ torus.BlockStore = &tempBlockStore{}

func init() {
	torus.RegisterBlockStore("temp", openTempBlockStore)
}

type tempBlockStore struct {
	mut       sync.RWMutex
	store     map[torus.BlockRef][]byte
	nBlocks   uint64
	name      string
	blockSize uint64
}

func openTempBlockStore(name string, cfg torus.Config, gmd torus.GlobalMetadata) (torus.BlockStore, error) {
	nBlocks := cfg.StorageSize / gmd.BlockSize
	promBlocksAvail.WithLabelValues(name).Set(float64(nBlocks))
	promBlocks.WithLabelValues(name).Set(0)
	promBytesPerBlock.Set(float64(gmd.BlockSize))
	return &tempBlockStore{
		store:     make(map[torus.BlockRef][]byte),
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

func (t *tempBlockStore) HasBlock(_ context.Context, s torus.BlockRef) (bool, error) {
	t.mut.Lock()
	defer t.mut.Unlock()
	_, ok := t.store[s]
	return ok, nil
}

func (t *tempBlockStore) GetBlock(_ context.Context, s torus.BlockRef) ([]byte, error) {
	t.mut.RLock()
	defer t.mut.RUnlock()

	if t.store == nil {
		promBlocksFailed.WithLabelValues(t.name).Inc()
		return nil, torus.ErrClosed
	}

	x, ok := t.store[s]
	if !ok {
		promBlocksFailed.WithLabelValues(t.name).Inc()
		return nil, torus.ErrBlockNotExist
	}
	promBlocksRetrieved.WithLabelValues(t.name).Inc()
	return x, nil
}

func (t *tempBlockStore) WriteBlock(_ context.Context, s torus.BlockRef, data []byte) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.store == nil {
		promBlockWritesFailed.WithLabelValues(t.name).Inc()
		return torus.ErrClosed
	}
	if int(t.nBlocks) <= len(t.store) {
		return torus.ErrOutOfSpace
	}
	buf := make([]byte, len(data))
	copy(buf, data)
	t.store[s] = buf
	promBlocks.WithLabelValues(t.name).Set(float64(len(t.store)))
	promBlocksWritten.WithLabelValues(t.name).Inc()
	return nil
}

func (t *tempBlockStore) WriteBuf(_ context.Context, s torus.BlockRef) ([]byte, error) {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.store == nil {
		promBlockWritesFailed.WithLabelValues(t.name).Inc()
		return nil, torus.ErrClosed
	}
	if int(t.nBlocks) <= len(t.store) {
		return nil, torus.ErrOutOfSpace
	}
	buf := make([]byte, t.blockSize)
	t.store[s] = buf
	promBlocks.WithLabelValues(t.name).Set(float64(len(t.store)))
	promBlocksWritten.WithLabelValues(t.name).Inc()
	return buf, nil
}

func (t *tempBlockStore) DeleteBlock(_ context.Context, s torus.BlockRef) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.store == nil {
		promBlockDeletesFailed.WithLabelValues(t.name).Inc()
		return torus.ErrClosed
	}

	delete(t.store, s)
	promBlocks.WithLabelValues(t.name).Set(float64(len(t.store)))
	promBlocksDeleted.WithLabelValues(t.name).Inc()
	return nil
}

func (t *tempBlockStore) BlockIterator() torus.BlockIterator {
	t.mut.RLock()
	defer t.mut.RUnlock()
	blocks := make([]torus.BlockRef, 0, len(t.store))
	for k := range t.store {
		blocks = append(blocks, k)
	}
	return &tempIterator{
		blocks: blocks,
		index:  -1,
	}
}

type tempIterator struct {
	blocks []torus.BlockRef
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

func (i *tempIterator) BlockRef() torus.BlockRef { return i.blocks[i.index] }

func (i *tempIterator) Close() error { return nil }
