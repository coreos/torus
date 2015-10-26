package storage

import (
	"errors"
	"os"

	"github.com/edsrzf/mmap-go"
)

type MFile struct {
	mmap    mmap.MMap
	blkSize uint64
	size    uint64
}

func CreateMFile(path string, size int64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	f.Truncate(size)
	return nil
}

func OpenMFile(path string, blkSize uint64) (*MFile, error) {
	var mf MFile

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	// We don't need the file handle after we mmap it.
	// See http://stackoverflow.com/questions/17490033/do-i-need-to-keep-a-file-open-after-calling-mmap-on-it.
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	mf.size = uint64(st.Size())
	if mf.size%blkSize != 0 {
		return nil, errors.New("File size is not a multiple of the block size")
	}
	mf.mmap, err = mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	mf.blkSize = blkSize
	return &mf, nil
}

// GetBlock returns the n-th block as a byte slice, including any trailing zero padding.
// The returned bytes are from the underlying mmap'd buffer and will be invalid after a call to Close().
func (m *MFile) GetBlock(n uint64) []byte {
	offset := n * m.blkSize
	if offset >= m.size {
		return nil
	}
	return m.mmap[offset : offset+m.blkSize]
}

// NumBlocks returns the total capacity of the file in blocks.
func (m *MFile) NumBlocks() uint64 {
	return m.size / m.blkSize
}

// WriteBlock writes data to the n-th block in the file.
func (m *MFile) WriteBlock(n uint64, data []byte) error {
	if uint64(len(data)) > m.blkSize {
		return errors.New("Data block too large")
	}
	offset := n * m.blkSize
	if offset >= m.size {
		return errors.New("Offset too large")
	}
	for i, b := range data {
		m.mmap[offset+uint64(i)] = b
	}
	// Fill the rest of the block with zeros.
	for i := uint64(len(data)); i < m.blkSize; i++ {
		m.mmap[offset+uint64(i)] = byte(0)
	}
	return nil
}

func (m *MFile) Flush() error {
	return m.mmap.Flush()
}

func (m *MFile) Close() error {
	if err := m.mmap.Flush(); err != nil {
		return err
	}
	return m.mmap.Unmap()
}
