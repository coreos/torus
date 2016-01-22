package block

import (
	"errors"
	"sync"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
)

var _ agro.BlockStore = &tempBlockStore{}

func init() {
	agro.RegisterBlockStore("temp", openTempBlockStore)
}

type tempBlockStore struct {
	mut     sync.RWMutex
	store   map[agro.BlockRef][]byte
	nBlocks uint64
	name    string
}

func openTempBlockStore(name string, cfg agro.Config, gmd agro.GlobalMetadata) (agro.BlockStore, error) {
	// TODO(barakmich): Currently we lie about the number of blocks.
	// If we want to guess at a size, or make the map be a max size, or something, PRs accepted.
	nBlocks := cfg.StorageSize / 1024
	promBlocksAvail.WithLabelValues(name).Set(float64(nBlocks))
	promBlocks.WithLabelValues(name).Set(0)
	promBytesPerBlock.Set(float64(gmd.BlockSize))
	return &tempBlockStore{
		store:   make(map[agro.BlockRef][]byte),
		nBlocks: nBlocks,
		name:    name,
	}, nil
}

func (t *tempBlockStore) Kind() string { return "temp" }
func (t *tempBlockStore) Flush() error { return nil }

func (t *tempBlockStore) Close() error {
	t.mut.Lock()
	if t.store != nil {
		t.store = nil
	}
	t.mut.Unlock()
	return nil
}

func (t *tempBlockStore) ReplaceBlockStore(bs agro.BlockStore) (agro.BlockStore, error) {
	if v, ok := bs.(*tempBlockStore); ok {
		v.name = t.name
		promBlocks.WithLabelValues(v.name).Set(float64(len(v.store)))
		promBlocksAvail.WithLabelValues(v.name).Set(float64(v.nBlocks))
		return v, nil
	}
	return nil, errors.New("not a tempBlockStore")
}

func (t *tempBlockStore) NumBlocks() uint64 {
	return t.nBlocks
}

func (t *tempBlockStore) UsedBlocks() uint64 {
	return uint64(len(t.store))
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

	t.store[s] = data
	promBlocks.WithLabelValues(t.name).Set(float64(len(t.store)))
	promBlocksWritten.WithLabelValues(t.name).Inc()
	return nil
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

func (t *tempBlockStore) DeleteINodeBlocks(_ context.Context, s agro.INodeRef) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.store == nil {
		return agro.ErrClosed
	}

	for k := range t.store {
		if k.HasINode(s, agro.Block) {
			promBlocksDeleted.WithLabelValues(t.name).Inc()
			delete(t.store, k)
		}
	}
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
