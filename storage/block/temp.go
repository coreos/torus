package block

import (
	"sync"

	"github.com/barakmich/agro"
)

type tempBlockStore struct {
	mut   sync.RWMutex
	store map[agro.BlockID][]byte
}

func CreateTempBlockStore() {}

func OpenTempBlockStore() agro.BlockStore {
	return &tempBlockStore{
		store: make(map[agro.BlockID][]byte),
	}
}

func (t *tempBlockStore) Flush() error { return nil }
func (t *tempBlockStore) Close() error {
	t.mut.Lock()
	t.store = nil
	t.mut.Unlock()
	return nil
}

func (t *tempBlockStore) GetBlock(s agro.BlockID) ([]byte, error) {
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

func (t *tempBlockStore) WriteBlock(s agro.BlockID, data []byte) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.store == nil {
		return agro.ErrClosed
	}

	t.store[s] = data
	return nil
}

func (t *tempBlockStore) DeleteBlock(s agro.BlockID) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.store == nil {
		return agro.ErrClosed
	}

	delete(t.store, s)
	return nil
}
