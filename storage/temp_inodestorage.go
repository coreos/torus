package storage

import (
	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
)

type tempINodeStore struct {
	store map[uint64]*models.INode
}

func CreateTempINodeStore() {}

func OpenTempINodeStore() agro.INodeStore {
	return &tempINodeStore{
		store: make(map[uint64]*models.INode),
	}
}

func (t *tempINodeStore) Flush() error { return nil }
func (t *tempINodeStore) Close() error {
	t.store = nil
	return nil
}

func (t *tempINodeStore) GetINode(i uint64) (*models.INode, error) {
	x, ok := t.store[i]
	if !ok {
		return nil, agro.ErrKeyNotFound
	}
	return x, nil
}

func (t *tempINodeStore) WriteINode(i uint64, inode *models.INode) error {
	t.store[i] = inode
	return nil
}

func (t *tempINodeStore) DeleteINode(i uint64) error {
	delete(t.store, i)
	return nil
}
