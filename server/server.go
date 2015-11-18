package server

import (
	"path"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
	"github.com/barakmich/agro/storage"
)

type server struct {
	cold     agro.BlockStore
	metadata agro.Metadata
	inodes   agro.INodeStore
}

func NewMemoryServer() agro.Server {
	mds := agro.CreateMetadata("temp", "")
	return &server{
		cold:     storage.OpenTempBlockStore(),
		metadata: mds,
		inodes:   storage.OpenTempINodeStore(),
	}
}

func (s *server) Create(path agro.Path, md models.Metadata) (agro.File, error) {
	// Truncate the file if it already exists. This is equivalent to creating
	// a new (empty) inode with the path that we're going to overwrite later.
	n := models.NewEmptyInode()
	n.Permissions = &md
	return &file{
		path:  path,
		inode: n,
		srv:   s,
	}, nil
}

func (s *server) Open(p agro.Path) (agro.File, error) {
	dir, _, err := s.metadata.Getdir(p)
	if err != nil {
		return nil, err
	}

	// TODO(jzelinskie): check metadata for permission

	_, filename := path.Split(p.Path)
	inode, err := s.inodes.GetINode(dir.Files[filename])
	if err != nil {
		return nil, err
	}

	return s.newFile(p, inode), nil
}

func (s *server) CreateVolume(vol string) error {
	return s.metadata.CreateVolume(vol)
}

func (s *server) GetVolumes() ([]string, error) {
	return s.metadata.GetVolumes()
}
