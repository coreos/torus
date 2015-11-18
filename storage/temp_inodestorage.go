package storage

import (
	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
)

type tempINodeStore struct {
	store map[agro.INodeRef]*models.INode
}

func CreateTempINodeStore() {}

func OpenTempINodeStore() agro.INodeStore {
	return &tempINodeStore{
		store: make(map[agro.INodeRef]*models.INode),
	}
}

func (t *tempINodeStore) Flush() error { return nil }
func (t *tempINodeStore) Close() error {
	t.store = nil
	return nil
}

func (t *tempINodeStore) GetINode(i agro.INodeRef) (*models.INode, error) {
	x, ok := t.store[i]
	if !ok {
		return nil, agro.ErrBlockNotExist
	}
	return x, nil
}

func (t *tempINodeStore) WriteINode(i agro.INodeRef, inode *models.INode) error {
	t.store[i] = inode
	return nil
}

func (t *tempINodeStore) DeleteINode(i agro.INodeRef) error {
	delete(t.store, i)
	return nil
}
