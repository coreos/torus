package fs

import (
	"os"
	"path"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

type FSServer interface {
	agro.Server

	// Standard file path calls.
	Create(Path) (File, error)
	Open(Path) (File, error)
	OpenFile(p Path, flag int, perm os.FileMode) (File, error)
	OpenFileMetadata(p Path, flag int, md *models.Metadata) (File, error)
	Rename(p Path, new Path) error
	Link(p Path, new Path) error
	Symlink(to string, new Path) error
	Lstat(Path) (os.FileInfo, error)
	Readdir(Path) ([]Path, error)
	Remove(Path) error
	Mkdir(Path, *models.Metadata) error

	Chmod(name Path, mode os.FileMode) error
	Chown(name Path, uid, gid int) error
}

type openFileCount struct {
	fh    *fileHandle
	count int
}

type server struct {
	*agro.Server
	openINodeRefs  map[string]map[agro.INodeID]int
	openFileChains map[uint64]openFileCount
}

func (s *server) FileEntryForPath(p agro.Path) (agro.VolumeID, *models.FileEntry, error) {
	promOps.WithLabelValues("file-entry-for-path").Inc()
	dirname, filename := path.Split(p.Path)
	dirpath := agro.Path{p.Volume, dirname}
	dir, _, err := s.fsMDS().Getdir(dirpath)
	if err != nil {
		return agro.VolumeID(0), nil, err
	}

	vol, err := s.mds.GetVolume(p.Volume)
	if err != nil {
		return 0, nil, err
	}
	ent, ok := dir.Files[filename]
	if !ok {
		return agro.VolumeID(vol.Id), nil, os.ErrNotExist
	}

	return agro.VolumeID(vol.Id), ent, nil
}

func (s *server) inodeRefForPath(p agro.Path) (agro.INodeRef, error) {
	vol, ent, err := s.FileEntryForPath(p)
	if err != nil {
		return agro.INodeRef{}, err
	}
	if ent.Sympath != "" {
		return s.inodeRefForPath(agro.Path{p.Volume, path.Clean(p.Base() + "/" + ent.Sympath)})
	}
	return s.fsMDS().GetChainINode(agro.NewINodeRef(vol, agro.INodeID(ent.Chain)))
}
