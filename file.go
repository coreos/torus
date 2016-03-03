package agro

import (
	"io"
	"os"
)

// File is the interface that represents the standardized methods to interact
// with a file in the filesystem.
type File interface {
	io.ReadWriter
	io.ReaderAt
	io.WriterAt
	io.Closer
	Sync() error
	Stat() (os.FileInfo, error)
	Truncate(int64) error
	Trim(int64, int64) error
}

type BlockFile interface {
	io.ReadWriter
	io.ReaderAt
	io.WriterAt
	io.Closer
	Sync() error
}
