package block

import (
	"bytes"
	"fmt"
	"path/filepath"

	"runtime"
	"sync"

	"golang.org/x/net/context"

	"github.com/betawaffle/trie"
	"github.com/coreos/agro"
	"github.com/coreos/agro/storage"
	"github.com/coreos/pkg/capnslog"
)

var _ agro.BlockStore = &mfileBlock{}

func init() {
	agro.RegisterBlockStore("mfile", newMFileBlockStore)
}

type mfileBlock struct {
	mut       sync.RWMutex
	data      *storage.MFile
	blockMap  *storage.MFile
	blockTrie *trie.Node
	closed    bool
	lastFree  int
	size      uint64
	dfilename string
	mfilename string
	name      string
	blocksize uint64
	// NB: Still room for improvement. Free lists, smart allocation, etc.
}

var blankRefBytes = make([]byte, agro.BlockRefByteSize)

func loadTrie(m *storage.MFile) (*trie.Node, uint64, error) {
	var t *trie.Node
	clog.Infof("loading trie...")
	size := uint64(0)
	var membefore uint64
	if clog.LevelAt(capnslog.DEBUG) {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		membefore = mem.Alloc
	}

	for i := uint64(0); i < m.NumBlocks(); i++ {
		b := m.GetBlock(i)
		if bytes.Equal(blankRefBytes, b) {
			continue
		}
		t = t.Put(b, int(i))
		size++
	}
	if clog.LevelAt(capnslog.DEBUG) {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		clog.Debugf("trie memory usage: %dK", ((mem.Alloc - membefore) / 1024))
	}
	clog.Infof("done loading trie")
	return t, size, nil
}

func newMFileBlockStore(name string, cfg agro.Config, meta agro.GlobalMetadata) (agro.BlockStore, error) {
	nBlocks := cfg.StorageSize / meta.BlockSize
	promBytesPerBlock.Set(float64(meta.BlockSize))
	promBlocksAvail.WithLabelValues(name).Set(float64(nBlocks))
	dpath := filepath.Join(cfg.DataDir, "block", fmt.Sprintf("data-%s.blk", name))
	mpath := filepath.Join(cfg.DataDir, "block", fmt.Sprintf("map-%s.blk", name))
	d, err := storage.CreateOrOpenMFile(dpath, cfg.StorageSize, meta.BlockSize)
	if err != nil {
		return nil, err
	}
	m, err := storage.CreateOrOpenMFile(mpath, nBlocks*agro.BlockRefByteSize, agro.BlockRefByteSize)
	if err != nil {
		return nil, err
	}
	trie, size, err := loadTrie(m)
	if err != nil {
		return nil, err
	}
	if m.NumBlocks() != d.NumBlocks() {
		panic("non-equal number of blocks between data and metadata")
	}
	promBlocks.WithLabelValues(name).Set(float64(size))
	return &mfileBlock{
		data:      d,
		blockMap:  m,
		blockTrie: trie,
		size:      size,
		dfilename: dpath,
		mfilename: mpath,
		name:      name,
		blocksize: meta.BlockSize,
	}, nil
}

func (m *mfileBlock) Kind() string { return "mfile" }
func (m *mfileBlock) NumBlocks() uint64 {
	m.mut.RLock()
	defer m.mut.RUnlock()
	return m.numBlocks()
}

func (m *mfileBlock) BlockSize() uint64 {
	return m.blocksize
}

func (m *mfileBlock) numBlocks() uint64 {
	return m.data.NumBlocks()
}

func (m *mfileBlock) UsedBlocks() uint64 {
	return m.size
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
	promStorageFlushes.WithLabelValues(m.name).Inc()
	return nil
}

func (m *mfileBlock) Close() error {
	m.mut.Lock()
	defer m.mut.Unlock()
	return m.close()
}

func (m *mfileBlock) close() error {
	m.Flush()
	if m.closed {
		return nil
	}
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
	if clog.LevelAt(capnslog.TRACE) {
		clog.Tracef("finding blockid %s", s)
	}
	if v, ok := m.blockTrie.Get(id).(int); ok {
		return v
	}
	return -1
}

func (m *mfileBlock) findEmpty() int {
	emptyBlock := make([]byte, agro.BlockRefByteSize)
	for i := uint64(0); i < m.numBlocks(); i++ {
		b := m.blockMap.GetBlock((i + uint64(m.lastFree) + 1) % m.numBlocks())
		if bytes.Equal(b, emptyBlock) {
			m.lastFree = int((i + uint64(m.lastFree) + 1) % m.numBlocks())
			return m.lastFree
		}
	}
	return -1
}

func (m *mfileBlock) HasBlock(_ context.Context, s agro.BlockRef) (bool, error) {
	m.mut.RLock()
	defer m.mut.RUnlock()
	index := m.findIndex(s)
	if index == -1 {
		return false, nil
	}
	return true, nil
}

func (m *mfileBlock) GetBlock(_ context.Context, s agro.BlockRef) ([]byte, error) {
	m.mut.RLock()
	defer m.mut.RUnlock()
	if m.closed {
		promBlocksFailed.WithLabelValues(m.name).Inc()
		return nil, agro.ErrClosed
	}
	index := m.findIndex(s)
	if index == -1 {
		promBlocksFailed.WithLabelValues(m.name).Inc()
		return nil, agro.ErrBlockNotExist
	}
	clog.Tracef("mfile: getting block at index %d", index)
	promBlocksRetrieved.WithLabelValues(m.name).Inc()
	return m.data.GetBlock(uint64(index)), nil
}

func (m *mfileBlock) WriteBlock(_ context.Context, s agro.BlockRef, data []byte) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	if m.closed {
		promBlockWritesFailed.WithLabelValues(m.name).Inc()
		return agro.ErrClosed
	}
	index := m.findEmpty()
	if index == -1 {
		clog.Error("mfile: out of space")
		promBlockWritesFailed.WithLabelValues(m.name).Inc()
		return agro.ErrOutOfSpace
	}
	clog.Tracef("mfile: writing block at index %d", index)
	err := m.data.WriteBlock(uint64(index), data)
	if err != nil {
		promBlockWritesFailed.WithLabelValues(m.name).Inc()
		return err
	}
	err = m.blockMap.WriteBlock(uint64(index), s.ToBytes())
	if err != nil {
		promBlockWritesFailed.WithLabelValues(m.name).Inc()
		return err
	}
	tx := m.blockTrie
	refbytes := s.ToBytes()
	if oldval := tx.Get(refbytes); oldval != nil {
		// we already have it
		clog.Debug("mfile: block already exists", s)
		olddata := m.data.GetBlock(uint64(oldval.(int)))
		if !bytes.Equal(olddata, data) {
			clog.Error("getting wrong data for block", s)
			clog.Errorf("%s, %s", olddata[:10], data[:10])
			return agro.ErrExists
		}
		// Not an error, if we already have it
		return nil
	}
	m.size++
	promBlocks.WithLabelValues(m.name).Inc()
	m.blockTrie = tx.Put(refbytes, index)
	promBlocksWritten.WithLabelValues(m.name).Inc()
	return nil
}

func (m *mfileBlock) WriteBuf(_ context.Context, s agro.BlockRef) ([]byte, error) {
	m.mut.Lock()
	defer m.mut.Unlock()
	if m.closed {
		promBlockWritesFailed.WithLabelValues(m.name).Inc()
		return nil, agro.ErrClosed
	}
	index := m.findEmpty()
	if index == -1 {
		clog.Error("mfile: out of space")
		promBlockWritesFailed.WithLabelValues(m.name).Inc()
		return nil, agro.ErrOutOfSpace
	}
	clog.Tracef("mfile: writing block at index %d", index)
	buf := m.data.GetBlock(uint64(index))
	err := m.blockMap.WriteBlock(uint64(index), s.ToBytes())
	if err != nil {
		promBlockWritesFailed.WithLabelValues(m.name).Inc()
		return nil, err
	}
	tx := m.blockTrie
	refbytes := s.ToBytes()
	if oldval := tx.Get(refbytes); oldval != nil {
		// we already have it
		clog.Debug("mfile: block already exists", s)
		// Not an error, if we already have it
		return nil, agro.ErrExists
	}
	m.size++
	promBlocks.WithLabelValues(m.name).Inc()
	m.blockTrie = tx.Put(refbytes, index)
	promBlocksWritten.WithLabelValues(m.name).Inc()
	return buf, nil
}

func (m *mfileBlock) DeleteBlock(_ context.Context, s agro.BlockRef) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	if m.closed {
		promBlockDeletesFailed.WithLabelValues(m.name).Inc()
		return agro.ErrClosed
	}
	index := m.findIndex(s)
	if index == -1 {
		promBlockDeletesFailed.WithLabelValues(m.name).Inc()
		clog.Errorf("mfile: deleting non-existent thing? %s", s)
		return agro.ErrBlockNotExist
	}
	err := m.blockMap.WriteBlock(uint64(index), blankRefBytes)
	if err != nil {
		promBlockDeletesFailed.WithLabelValues(m.name).Inc()
		return err
	}
	m.size--
	promBlocks.WithLabelValues(m.name).Dec()
	m.blockTrie = m.blockTrie.Delete(s.ToBytes())
	promBlocksDeleted.WithLabelValues(m.name).Inc()
	return nil
}

func (m *mfileBlock) BlockIterator() agro.BlockIterator {
	m.mut.Lock()
	defer m.mut.Unlock()
	return &mfileIterator{
		tx: m.blockTrie,
	}
}

type mfileIterator struct {
	tx     *trie.Node
	err    error
	c      chan *trie.Node
	result *trie.Node
	done   bool
}

func (i *mfileIterator) Err() error { return i.err }

func (i *mfileIterator) Next() bool {
	if i.done {
		return false
	}
	if i.c == nil {
		i.c = make(chan *trie.Node)
		go i.tx.WalkChan(i.c)
	}
	var ok bool
	i.result, ok = <-i.c
	if !ok {
		i.done = true
	}
	return ok
}

func (i *mfileIterator) BlockRef() agro.BlockRef {
	return agro.BlockRefFromBytes(i.result.Key())
}

func (i *mfileIterator) Close() error {
	if i.done {
		return nil
	}
	for _ = range i.c {
	}
	i.done = true
	return nil
}
