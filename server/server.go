package server

import (
	"os"
	"path"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
	"github.com/barakmich/agro/storage"
)

type server struct {
	cold   agro.BlockStore
	mds    agro.MetadataService
	inodes agro.INodeStore
}

func NewMemoryServer() agro.Server {
	mds := agro.CreateMetadataService("temp", "")
	return &server{
		cold:   storage.OpenTempBlockStore(),
		mds:    mds,
		inodes: storage.OpenTempINodeStore(),
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
	ref, err := s.inodeRefForPath(p)
	if err != nil {
		return nil, err
	}

	inode, err := s.inodes.GetINode(ref)
	if err != nil {
		return nil, err
	}

	// TODO(jzelinskie): check metadata for permission

	return s.newFile(p, inode), nil
}

func (s *server) inodeRefForPath(p agro.Path) (agro.INodeRef, error) {
	dir, _, err := s.mds.Getdir(p)
	if err != nil {
		return agro.INodeRef{}, err
	}

	volID, err := s.mds.GetVolumeID(p.Volume)
	if err != nil {
		return agro.INodeRef{}, err
	}

	_, filename := path.Split(p.Path)
	inodeID, ok := dir.Files[filename]
	if !ok {
		return agro.INodeRef{}, os.ErrNotExist
	}

	return agro.INodeRef{volID, agro.INodeID(inodeID)}, nil
}

func (s *server) CreateVolume(vol string) error {
	return s.mds.CreateVolume(vol)
}

func (s *server) GetVolumes() ([]string, error) {
	return s.mds.GetVolumes()
}
