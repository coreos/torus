package server

import (
	"io"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/types"
)

type file struct {
	path   agro.Path
	inode  *types.INode
	srv    *server
	offset int64
}

func (f *file) Write(b []byte) (n int, err error) {
	return 0, nil
}

func (f *file) Read(b []byte) (n int, err error) {
	return 0, io.EOF
}
