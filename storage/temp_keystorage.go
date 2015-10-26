package storage

import "github.com/barakmich/agro"

type tempKeyStore struct {
	store map[string][]byte
}

func CreateTempKeyStore() {}

func OpenTempKeyStore() agro.KeyStore {
	return &tempKeyStore{
		store: make(map[string][]byte),
	}
}

func (t *tempKeyStore) Flush() error { return nil }
func (t *tempKeyStore) Close() error {
	t.store = nil
	return nil
}

func (t *tempKeyStore) GetKey(s string) ([]byte, error) {
	x, ok := t.store[s]
	if !ok {
		return nil, agro.ErrKeyNotFound
	}
	return x, nil
}

func (t *tempKeyStore) WriteKey(s string, data []byte) error {
	t.store[s] = data
	return nil
}

func (t *tempKeyStore) DeleteKey(s string) error {
	delete(t.store, s)
	return nil
}
