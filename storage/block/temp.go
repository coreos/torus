package block

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/barakmich/agro"
)

var _ agro.BlockStore = &tempBlockStore{}

func init() {
	agro.RegisterBlockStore("temp", openTempBlockStore)
}

type tempBlockStore struct {
	mut     sync.RWMutex
	store   map[agro.BlockRef][]byte
	nBlocks uint64
}

func openTempBlockStore(cfg agro.Config, gmd agro.GlobalMetadata) (agro.BlockStore, error) {
	return &tempBlockStore{
		store: make(map[agro.BlockRef][]byte),
		// TODO(barakmich): Currently we lie about the number of blocks.
		// If we want to guess at a size, or make the map be a max size, or something, PRs accepted.
		nBlocks: cfg.StorageSize / 1024,
	}, nil
}

func (t *tempBlockStore) Flush() error { return nil }

func (t *tempBlockStore) Close() error {
	t.mut.Lock()
	t.store = nil
	t.mut.Unlock()
	return nil
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
		return nil, agro.ErrClosed
	}

	x, ok := t.store[s]
	if !ok {
		return nil, agro.ErrBlockNotExist
	}
	return x, nil
}

func (t *tempBlockStore) WriteBlock(_ context.Context, s agro.BlockRef, data []byte) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.store == nil {
		return agro.ErrClosed
	}

	t.store[s] = data
	return nil
}

func (t *tempBlockStore) DeleteBlock(_ context.Context, s agro.BlockRef) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.store == nil {
		return agro.ErrClosed
	}

	delete(t.store, s)
	return nil
}
