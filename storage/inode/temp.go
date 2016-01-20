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
	name  string
}

func openTempINodeStore(name string, cfg agro.Config) (agro.INodeStore, error) {
	promINodes.WithLabelValues(name).Set(0)
	return &tempINodeStore{
		store: make(map[agro.INodeRef]*models.INode),
		name:  name,
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
		promINodesFailed.WithLabelValues(t.name).Inc()
		return nil, agro.ErrClosed
	}

	x, ok := t.store[i]
	if !ok {
		promINodesFailed.WithLabelValues(t.name).Inc()
		return nil, agro.ErrBlockNotExist
	}
	promINodesRetrieved.WithLabelValues(t.name).Inc()
	return x, nil
}

func (t *tempINodeStore) WriteINode(_ context.Context, i agro.INodeRef, inode *models.INode) error {
	t.mut.RLock()
	defer t.mut.RUnlock()

	if t.store == nil {
		promINodeWritesFailed.WithLabelValues(t.name).Inc()
		return agro.ErrClosed
	}

	t.store[i] = inode
	promINodesWritten.WithLabelValues(t.name).Inc()
	promINodes.WithLabelValues(t.name).Inc()
	return nil
}

func (t *tempINodeStore) DeleteINode(_ context.Context, i agro.INodeRef) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.store == nil {
		promINodeDeletesFailed.WithLabelValues(t.name).Inc()
		return agro.ErrClosed
	}

	delete(t.store, i)
	promINodesDeleted.WithLabelValues(t.name).Inc()
	promINodes.WithLabelValues(t.name).Dec()
	return nil
}

func (t *tempINodeStore) ReplaceINodeStore(is agro.INodeStore) (agro.INodeStore, error) {
	if v, ok := is.(*tempINodeStore); ok {
		v.name = t.name
		promINodes.WithLabelValues(v.name).Set(float64(len(v.store)))
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
