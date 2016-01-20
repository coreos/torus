package inode

import (
	"errors"
	"sync"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

var _ agro.INodeStore = &tempINodeStore{}

func init() {
	agro.RegisterINodeStore("temp", openTempINodeStore)
}

type tempINodeStore struct {
	mut   sync.RWMutex
	store map[agro.INodeRef]*models.INode
}

func openTempINodeStore(_ string, cfg agro.Config) (agro.INodeStore, error) {
	return &tempINodeStore{
		store: make(map[agro.INodeRef]*models.INode),
	}, nil
}

func (t *tempINodeStore) Kind() string { return "temp" }
func (t *tempINodeStore) Flush() error { return nil }
func (t *tempINodeStore) Close() error {
	t.mut.Lock()
	t.store = nil
	t.mut.Unlock()
	return nil
}

func (t *tempINodeStore) GetINode(_ context.Context, i agro.INodeRef) (*models.INode, error) {
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

func (t *tempINodeStore) WriteINode(_ context.Context, i agro.INodeRef, inode *models.INode) error {
	t.mut.RLock()
	defer t.mut.RUnlock()

	if t.store == nil {
		return agro.ErrClosed
	}

	t.store[i] = inode
	return nil
}

func (t *tempINodeStore) DeleteINode(_ context.Context, i agro.INodeRef) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.store == nil {
		return agro.ErrClosed
	}

	delete(t.store, i)
	return nil
}

func (t *tempINodeStore) ReplaceINodeStore(is agro.INodeStore) (agro.INodeStore, error) {
	if v, ok := is.(*tempINodeStore); ok {
		return v, nil
	}
	return nil, errors.New("not a tempINodeStore")
}

func (t *tempINodeStore) INodeIterator() agro.INodeIterator {
	t.mut.RLock()
	defer t.mut.RUnlock()
	var inodes []agro.INodeRef
	for k := range t.store {
		inodes = append(inodes, k)
	}
	return &tempIterator{
		inodes: inodes,
		index:  -1,
	}
}

type tempIterator struct {
	inodes []agro.INodeRef
	index  int
}

func (i *tempIterator) Err() error { return nil }

func (i *tempIterator) Next() bool {
	i.index++
	if i.index >= len(i.inodes) {
		return false
	}
	return true
}

func (i *tempIterator) INodeRef() agro.INodeRef { return i.inodes[i.index] }

func (i *tempIterator) Close() error { return nil }
