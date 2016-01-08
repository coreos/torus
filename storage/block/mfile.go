package block

import (
	"bytes"
	"errors"
	"path/filepath"
	"runtime"
	"sync"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
	"github.com/coreos/agro/storage"
	"github.com/coreos/pkg/capnslog"
	"github.com/hashicorp/go-immutable-radix"
)

var _ agro.BlockStore = &mfileBlock{}

func init() {
	agro.RegisterBlockStore("mfile", newMFileBlockStore)
}

type mfileBlock struct {
	mut       sync.RWMutex
	data      *storage.MFile
	blockMap  *storage.MFile
	blockTrie *iradix.Tree
	closed    bool
	lastFree  int
	// NB: Still room for improvement. Free lists, smart allocation, etc.
}

func loadTrie(m *storage.MFile) (*iradix.Tree, error) {
	t := iradix.New()
	tx := t.Txn()
	clog.Infof("loading trie...")
	var membefore uint64
	if clog.LevelAt(capnslog.DEBUG) {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		membefore = mem.Alloc
	}

	blank := make([]byte, agro.BlockRefByteSize)
	for i := uint64(0); i < m.NumBlocks(); i++ {
		b := m.GetBlock(i)
		if bytes.Equal(blank, b) {
			continue
		}
		tx.Insert(b, int(i))
	}
	if clog.LevelAt(capnslog.DEBUG) {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		clog.Debugf("trie memory usage: %dK", ((mem.Alloc - membefore) / 1024))
	}
	clog.Infof("done loading trie")
	return tx.Commit(), nil
}

func newMFileBlockStore(cfg agro.Config, meta agro.GlobalMetadata) (agro.BlockStore, error) {
	nBlocks := cfg.StorageSize / meta.BlockSize
	dpath := filepath.Join(cfg.DataDir, "block", "data.blk")
	mpath := filepath.Join(cfg.DataDir, "block", "map.blk")
	d, err := storage.CreateOrOpenMFile(dpath, cfg.StorageSize, meta.BlockSize)
	if err != nil {
		return nil, err
	}
	m, err := storage.CreateOrOpenMFile(mpath, nBlocks*agro.BlockRefByteSize, agro.BlockRefByteSize)
	if err != nil {
		return nil, err
	}
	trie, err := loadTrie(m)
	if err != nil {
		return nil, err
	}
	if m.NumBlocks() != d.NumBlocks() {
		panic("non-equal number of blocks between data and metadata")
	}
	return &mfileBlock{
		data:      d,
		blockMap:  m,
		blockTrie: trie,
	}, nil
}

func (m *mfileBlock) NumBlocks() uint64 {
	return m.data.NumBlocks()
}

func (m *mfileBlock) UsedBlocks() uint64 {
	return uint64(m.blockTrie.Len())
}

func (m *mfileBlock) Flush() error {
	err := m.data.Flush()
	if err != nil {
		return err
	}
	err = m.blockMap.Flush()
	if err != nil {
		return err
	}
	return nil
}

func (m *mfileBlock) Close() error {
	m.mut.Lock()
	defer m.mut.Unlock()
	m.Flush()
	err := m.data.Close()
	if err != nil {
		return err
	}
	err = m.blockMap.Close()
	if err != nil {
		return err
	}
	m.closed = true
	return nil
}

func (m *mfileBlock) findIndex(s agro.BlockRef) int {
	id := s.ToBytes()
	clog.Tracef("finding blockid %s, bytes %v", s, id)
	if v, ok := m.blockTrie.Get(id); ok {
		return v.(int)
	}
	return -1
}

func (m *mfileBlock) findEmpty() int {
	emptyBlock := make([]byte, agro.BlockRefByteSize)
	for i := uint64(0); i < m.NumBlocks(); i++ {
		b := m.blockMap.GetBlock((i + uint64(m.lastFree) + 1) % m.NumBlocks())
		if bytes.Equal(b, emptyBlock) {
			m.lastFree = int((i + uint64(m.lastFree) + 1) % m.NumBlocks())
			return m.lastFree
		}
	}
	return -1
}

func (m *mfileBlock) GetBlock(_ context.Context, s agro.BlockRef) ([]byte, error) {
	m.mut.RLock()
	defer m.mut.RUnlock()
	if m.closed {
		return nil, agro.ErrClosed
	}
	index := m.findIndex(s)
	if index == -1 {
		return nil, agro.ErrBlockNotExist
	}
	clog.Tracef("mfile: getting block at index %d", index)
	return m.data.GetBlock(uint64(index)), nil
}

func (m *mfileBlock) WriteBlock(_ context.Context, s agro.BlockRef, data []byte) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	if m.closed {
		return agro.ErrClosed
	}
	index := m.findEmpty()
	if index == -1 {
		clog.Error("mfile: out of space")
		return agro.ErrOutOfSpace
	}
	clog.Tracef("mfile: writing block at index %d", index)
	err := m.data.WriteBlock(uint64(index), data)
	if err != nil {
		return err
	}
	err = m.blockMap.WriteBlock(uint64(index), s.ToBytes())
	if err != nil {
		return err
	}

	tx := m.blockTrie.Txn()
	_, exists := tx.Insert(s.ToBytes(), index)
	if exists {
		return errors.New("mfile: block already existed?")
	}
	m.blockTrie = tx.Commit()
	return nil
}

func (m *mfileBlock) DeleteBlock(_ context.Context, s agro.BlockRef) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	if m.closed {
		return agro.ErrClosed
	}
	index := m.findIndex(s)
	if index == -1 {
		return agro.ErrBlockNotExist
	}
	err := m.blockMap.WriteBlock(uint64(index), make([]byte, agro.BlockRefByteSize))
	if err != nil {
		return err
	}
	tx := m.blockTrie.Txn()
	_, exists := tx.Delete(s.ToBytes())
	if !exists {
		return errors.New("mfile: deleting non-existent thing?")
	}
	m.blockTrie = tx.Commit()
	return nil
}

func (m *mfileBlock) DeleteINodeBlocks(_ context.Context, s agro.INodeRef) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	if m.closed {
		return agro.ErrClosed
	}
	tx := m.blockTrie.Txn()
	it := tx.Root().Iterator()
	it.SeekPrefix(s.ToBytes())
	var deleteList []int
	for {
		key, value, ok := it.Next()
		if !ok {
			break
		}
		deleteList = append(deleteList, value.(int))
		tx.Delete(key)
	}
	for _, v := range deleteList {
		err := m.blockMap.WriteBlock(uint64(v), make([]byte, agro.BlockRefByteSize))
		if err != nil {
			return err
		}
	}
	m.blockTrie = tx.Commit()
	return nil
}
