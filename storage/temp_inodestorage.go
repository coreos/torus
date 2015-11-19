package storage

import (
	"sync"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
)

type tempINodeStore struct {
	mut   sync.RWMutex
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
	t.mut.Lock()
	t.store = nil
	t.mut.Unlock()
	return nil
}

func (t *tempINodeStore) GetINode(i agro.INodeRef) (*models.INode, error) {
	t.mut.RLock()
	defer t.mut.RUnlock()

	if t.store == nil {
		return nil, agro.ErrClosed
	}

	x, ok := t.store[i]
	if !ok {
		return nil, agro.ErrBlockNotExist
	}
	return x, nil
}

func (t *tempINodeStore) WriteINode(i agro.INodeRef, inode *models.INode) error {
	t.mut.RLock()
	defer t.mut.RUnlock()

	if t.store == nil {
		return agro.ErrClosed
	}

	t.store[i] = inode
	return nil
}

func (t *tempINodeStore) DeleteINode(i agro.INodeRef) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.store == nil {
		return agro.ErrClosed
	}

	delete(t.store, i)
	return nil
}
