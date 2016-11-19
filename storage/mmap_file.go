package storage

import (
	"errors"
	"fmt"
	"os"

	"github.com/barakmich/mmap-go"
)

type MFile struct {
	mmap    mmap.MMap
	blkSize uint64
	size    uint64
}

func CreateOrOpenMFile(path string, size uint64, blkSize uint64) (*MFile, error) {
	finfo, err := os.Stat(path)
	if os.IsNotExist(err) {
		err := CreateMFile(path, size)
		if err != nil {
			return nil, err
		}
	}
	if finfo != nil && finfo.Size() != int64(size) {
		if finfo.Size() > int64(size) {
			return nil, fmt.Errorf("Specified size %d is smaller than current size %d.", size, finfo.Size())
		}
		clog.Debugf("mfile: expand %s %d to %d", path, finfo.Size(), size)
		os.Truncate(path, int64(size))
	}
	return OpenMFile(path, blkSize)
}

func CreateMFile(path string, size uint64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(int64(size))
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
		return nil, fmt.Errorf("File size is not a multiple of the block size: %d size, %d blksize", mf.size, blkSize)
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
	size := uint64(len(data))
	if size > m.blkSize {
		return errors.New("Data block too large")
	}
	if size == m.blkSize {
		offset := n * m.blkSize
		copy(m.mmap[offset:], data)
		return nil
	}

	blk := m.GetBlock(n)
	if blk == nil {
		return errors.New("Offset too large")
	}

	// Copy the data and fill the rest of the block with zeros.
	zero(blk[copy(blk, data):])
	return nil
}

func (m *MFile) Flush() error {
	return m.mmap.FlushAsync()
}

func (m *MFile) Close() error {
	if err := m.mmap.Flush(); err != nil {
		return err
	}
	return m.mmap.Unmap()
}

func zero(b []byte) {
	zeros := make([]byte, 8*1024)
	for len(b) > 0 {
		b = b[copy(b, zeros):]
	}
}
