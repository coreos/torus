package server

import (
	"errors"
	"os"
	"path"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/models"
	"github.com/tgruben/roaring"

	// Register drivers
	_ "github.com/coreos/agro/metadata/memory"
	_ "github.com/coreos/agro/storage/block"
	_ "github.com/coreos/agro/storage/inode"
)

var _ agro.Server = &server{}

type server struct {
	mut           sync.RWMutex
	blocks        agro.BlockStore
	mds           agro.MetadataService
	inodes        agro.INodeStore
	peersMap      map[string]*models.PeerInfo
	closeChans    []chan interface{}
	openINodeRefs map[string]map[agro.INodeID]int

	internalAddr string

	heartbeating    bool
	replicationOpen bool
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
	bs, err := blockset.CreateBlocksetFromSpec(globals.DefaultBlockSpec, s.blocks)
	if err != nil {
		return nil, err
	}
	clog.Tracef("Create file %s at inode %d:%d with block length %d", path, n.Volume, n.INode, bs.Length())
	return &file{
		path:          path,
		inode:         n,
		srv:           s,
		blocks:        bs,
		blkSize:       int64(globals.BlockSize),
		initialINodes: roaring.NewRoaringBitmap(),
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

type fileInfo struct {
	inode *models.INode
	path  agro.Path
}

func (fi fileInfo) Name() string {
	return fi.inode.Filenames[0]
}

func (fi fileInfo) Size() int64 {
	return int64(fi.inode.Filesize)
}

func (fi fileInfo) Mode() os.FileMode {
	return os.FileMode(fi.inode.Permissions.Mode)
}

func (fi fileInfo) ModTime() time.Time {
	return time.Unix(0, int64(fi.inode.Permissions.Mtime))
}

func (fi fileInfo) IsDir() bool {
	return fi.path.IsDir()
}

func (fi fileInfo) Sys() interface{} {
	return fi
}

func (s *server) Lstat(path agro.Path) (os.FileInfo, error) {
	ref, err := s.inodeRefForPath(path)
	if err != nil {
		return nil, err
	}

	inode, err := s.inodes.GetINode(context.TODO(), ref)
	if err != nil {
		return nil, err
	}

	return fileInfo{inode, path}, nil
}

func (s *server) Readdir(path agro.Path) ([]agro.Path, error) {
	if !path.IsDir() {
		return nil, errors.New("ENOTDIR")
	}

	dir, subdirs, err := s.mds.Getdir(path)
	if err != nil {
		return nil, err
	}

	var entries []agro.Path
	entries = append(entries, subdirs...)

	for filename := range dir.Files {
		childPath, ok := path.Child(filename)
		if !ok {
			return nil, errors.New("server: entry path is not a directory")
		}

		entries = append(entries, childPath)
	}

	return entries, nil
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
		clog.Errorf("couldn't close mds: %s", err)
		return err
	}
	err = s.inodes.Close()
	if err != nil {
		clog.Errorf("couldn't close inodes: %s", err)
		return err
	}
	err = s.blocks.Close()
	if err != nil {
		clog.Errorf("couldn't close blocks: %s", err)
		return err
	}
	return nil
}

func (s *server) incRef(vol string, bm *roaring.RoaringBitmap) {
	s.mut.Lock()
	defer s.mut.Unlock()
	if bm.GetCardinality() == 0 {
		return
	}
	if _, ok := s.openINodeRefs[vol]; !ok {
		s.openINodeRefs[vol] = make(map[agro.INodeID]int)
	}
	it := bm.Iterator()
	for it.HasNext() {
		id := agro.INodeID(it.Next())
		v, ok := s.openINodeRefs[vol][id]
		if !ok {
			s.openINodeRefs[vol][id] = 1
		} else {
			s.openINodeRefs[vol][id] = v + 1
		}
	}
}

func (s *server) decRef(vol string, bm *roaring.RoaringBitmap) {
	s.mut.Lock()
	defer s.mut.Unlock()
	it := bm.Iterator()
	for it.HasNext() {
		id := agro.INodeID(it.Next())
		v, ok := s.openINodeRefs[vol][id]
		if !ok {
			panic("server: double remove of an inode reference")
		} else {
			v--
			if v == 0 {
				delete(s.openINodeRefs[vol], id)
			} else {
				s.openINodeRefs[vol][id] = v
			}
		}
	}
	if len(s.openINodeRefs[vol]) == 0 {
		delete(s.openINodeRefs, vol)
	}
}

func (s *server) getBitmap(vol string) (*roaring.RoaringBitmap, bool) {
	s.mut.Lock()
	defer s.mut.Unlock()
	if _, ok := s.openINodeRefs[vol]; !ok {
		return nil, false
	}
	out := roaring.NewRoaringBitmap()
	for k := range s.openINodeRefs[vol] {
		out.Add(uint32(k))
	}
	return out, true
}
