package server

import (
	"github.com/barakmich/agro"
	"github.com/barakmich/agro/storage"
	"github.com/barakmich/agro/types"
)

type server struct {
	cold     agro.KeyStore
	metadata agro.Metadata
	inodes   agro.INodeStore
}

func NewMemoryServer() agro.Server {
	mds := agro.CreateMetadata("temp", "")
	return &server{
		cold:     storage.OpenTempKeyStore(),
		metadata: mds,
		inodes:   storage.OpenTempINodeStore(),
	}
}

func (s *server) Create(path agro.Path, md types.Metadata) (agro.File, error) {
	// Truncate the file if it already exists. This is equivalent to creating
	// a new (empty) inode with the path that we're going to overwrite later.
	n := types.NewEmptyInode()
	n.Permissions = &md
	return &file{
		path:  path,
		inode: n,
		srv:   s,
	}, nil
}
