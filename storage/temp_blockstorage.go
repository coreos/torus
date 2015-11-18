package storage

import "github.com/barakmich/agro"

type tempBlockStore struct {
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
	t.store = nil
	return nil
}

func (t *tempBlockStore) GetBlock(s agro.BlockID) ([]byte, error) {
	x, ok := t.store[s]
	if !ok {
		return nil, agro.ErrBlockNotExist
	}
	return x, nil
}

func (t *tempBlockStore) WriteBlock(s agro.BlockID, data []byte) error {
	t.store[s] = data
	return nil
}

func (t *tempBlockStore) DeleteBlock(s agro.BlockID) error {
	delete(t.store, s)
	return nil
}
