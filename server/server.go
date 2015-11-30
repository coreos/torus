package server

import (
	"os"
	"path"

	"golang.org/x/net/context"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/blockset"
	"github.com/barakmich/agro/models"

	// Register drivers
	_ "github.com/barakmich/agro/metadata/temp"
	_ "github.com/barakmich/agro/storage/block"
	_ "github.com/barakmich/agro/storage/inode"
)

type server struct {
	cold         agro.BlockStore
	mds          agro.MetadataService
	inodes       agro.INodeStore
	closeChans   []chan interface{}
	internalAddr string
}

func NewMemoryServer() agro.Server {
	cfg := agro.Config{}
	mds, _ := agro.CreateMetadataService("temp", cfg)
	inodes, _ := agro.CreateINodeStore("temp", cfg)
	gmd, _ := mds.GlobalMetadata()
	cold, _ := agro.CreateBlockStore("temp", cfg, gmd)
	return &server{
		cold:   cold,
		mds:    mds,
		inodes: inodes,
	}
}

func (s *server) Create(path agro.Path, md models.Metadata) (f agro.File, err error) {
	// Truncate the file if it already exists. This is equivalent to creating
	// a new (empty) inode with the path that we're going to overwrite later.
	n := models.NewEmptyInode()
	n.Filenames = []string{path.Path}
	volid, err := s.mds.GetVolumeID(path.Volume)
	n.Volume = uint64(volid)
	if err != nil {
		return nil, err
	}
	n.Permissions = &md
	globals, err := s.mds.GlobalMetadata()
	if err != nil {
		return nil, err
	}
	bs, err := blockset.CreateBlocksetFromSpec(globals.DefaultBlockSpec, s.cold)
	if err != nil {
		return nil, err
	}
	clog.Tracef("Create file %s at inode %d:%d with block length %d", path, n.Volume, n.Inode, bs.Length())
	return &file{
		path:    path,
		inode:   n,
		srv:     s,
		blocks:  bs,
		blkSize: int64(globals.BlockSize),
	}, nil
}

func (s *server) Open(p agro.Path) (agro.File, error) {
	ref, err := s.inodeRefForPath(p)
	if err != nil {
		return nil, err
	}

	inode, err := s.inodes.GetINode(context.TODO(), ref)
	if err != nil {
		return nil, err
	}

	// TODO(jzelinskie): check metadata for permission

	return s.newFile(p, inode)
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
	err := s.mds.CreateVolume(vol)
	if err == agro.ErrAgain {
		return s.CreateVolume(vol)
	}
	return err
}

func (s *server) GetVolumes() ([]string, error) {
	return s.mds.GetVolumes()
}

func (s *server) Close() error {
	for _, c := range s.closeChans {
		close(c)
	}
	err := s.mds.Close()
	if err != nil {
		return err
	}
	err = s.inodes.Close()
	if err != nil {
		return err
	}
	err = s.cold.Close()
	if err != nil {
		return err
	}
	return nil
}
