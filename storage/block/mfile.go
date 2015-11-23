package block

import (
	"bytes"
	"path/filepath"
	"sync"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/storage"
)

type mfileBlock struct {
	mut      sync.RWMutex
	data     *storage.MFile
	blockMap *storage.MFile
	nBlocks  int
	closed   bool
	// NB: This implementation is *super* naive to begin with.
	// There's no indexing, no smart allocation, no shortening of the block map, nothing cool.
	// Does the stupidest thing that works. Please improve.
}

const metaBytesPerBlock = 8 * 3 // int64 * 3 == agro.BlockID

func NewMFileBlockStorage(cfg agro.Config, meta agro.GlobalMetadata) (agro.BlockStore, error) {
	nBlocks := cfg.StorageSize / meta.BlockSize
	dpath := filepath.Join(cfg.DataDir, "block", "data.blk")
	mpath := filepath.Join(cfg.DataDir, "block", "map.blk")
	d, err := storage.CreateOrOpenMFile(dpath, cfg.StorageSize, meta.BlockSize)
	if err != nil {
		return nil, err
	}
	m, err := storage.CreateOrOpenMFile(mpath, nBlocks*metaBytesPerBlock, metaBytesPerBlock)
	if err != nil {
		return nil, err
	}
	return &mfileBlock{
		data:     d,
		blockMap: m,
		nBlocks:  int(nBlocks),
	}, nil
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

func (m *mfileBlock) findIndex(s agro.BlockID) int {
	id := s.ToBytes()
	for i := 0; i < m.nBlocks; i++ {
		b := m.blockMap.GetBlock(uint64(i))
		if bytes.HasPrefix(b, id) {
			return i
		}
	}
	return -1
}

func (m *mfileBlock) findEmpty() int {
	emptyBlock := make([]byte, metaBytesPerBlock)
	for i := 0; i < m.nBlocks; i++ {
		b := m.blockMap.GetBlock(uint64(i))
		if bytes.Equal(b, emptyBlock) {
			return i
		}
	}
	return -1
}

func (m *mfileBlock) GetBlock(s agro.BlockID) ([]byte, error) {
	m.mut.RLock()
	defer m.mut.RUnlock()
	if m.closed {
		return nil, agro.ErrClosed
	}
	index := m.findIndex(s)
	if index == -1 {
		return nil, agro.ErrBlockNotExist
	}
	return m.data.GetBlock(uint64(index)), nil
}

func (m *mfileBlock) WriteBlock(s agro.BlockID, data []byte) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	if m.closed {
		return agro.ErrClosed
	}
	index := m.findEmpty()
	if index == -1 {
		return agro.ErrOutOfSpace
	}
	return m.data.WriteBlock(uint64(index), data)
}

func (m *mfileBlock) DeleteBlock(s agro.BlockID) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	if m.closed {
		return agro.ErrClosed
	}
	index := m.findIndex(s)
	if index == -1 {
		return agro.ErrBlockNotExist
	}
	return m.blockMap.WriteBlock(uint64(index), make([]byte, metaBytesPerBlock))
}
