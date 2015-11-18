package server

import (
	"io"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
)

type file struct {
	path   agro.Path
	inode  *models.INode
	srv    *server
	offset int64
}

func (s *server) newFile(path agro.Path, inode *models.INode) agro.File {
	return &file{
		path:   path,
		inode:  inode,
		srv:    s,
		offset: 0,
	}
}

func (f *file) Write(b []byte) (n int, err error) {
	return 0, nil
}

func (f *file) Read(b []byte) (n int, err error) {
	return 0, io.EOF
}
