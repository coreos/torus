package server

import (
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/prometheus/client_golang/prometheus"

	// Register drivers
	_ "github.com/coreos/agro/metadata/memory"
	_ "github.com/coreos/agro/storage/block"
)

var (
	_       agro.Server = &server{}
	promOps             = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_server_ops_total",
		Help: "Number of times an atomic update failed and needed to be retried",
	}, []string{"kind"})
)

func init() {
	prometheus.MustRegister(promOps)
}

type openFileCount struct {
	fh    *fileHandle
	count int
}

type server struct {
	mut            sync.RWMutex
	writeableLock  sync.RWMutex
	infoMut        sync.Mutex
	blocks         agro.BlockStore
	mds            agro.MetadataService
	inodes         *INodeStore
	peersMap       map[string]*models.PeerInfo
	closeChans     []chan interface{}
	openINodeRefs  map[string]map[agro.INodeID]int
	openFileChains map[uint64]openFileCount
	cfg            agro.Config
	peerInfo       *models.PeerInfo

	lease            int64
	heartbeating     bool
	replicationOpen  bool
	timeoutCallbacks []func(string)
}

func (s *server) FileEntryForPath(p agro.Path) (agro.VolumeID, *models.FileEntry, error) {
	promOps.WithLabelValues("file-entry-for-path").Inc()
	dirname, filename := path.Split(p.Path)
	dirpath := agro.Path{p.Volume, dirname}
	dir, _, err := s.mds.Getdir(dirpath)
	if err != nil {
		return agro.VolumeID(0), nil, err
	}

	volID, err := s.mds.GetVolumeID(p.Volume)
	if err != nil {
		return volID, nil, err
	}

	ent, ok := dir.Files[filename]
	if !ok {
		return volID, nil, os.ErrNotExist
	}

	return volID, ent, nil
}

func (s *server) inodeRefForPath(p agro.Path) (agro.INodeRef, error) {
	vol, ent, err := s.FileEntryForPath(p)
	if err != nil {
		return agro.INodeRef{}, err
	}
	if ent.Sympath != "" {
		return s.inodeRefForPath(agro.Path{p.Volume, path.Clean(p.Base() + "/" + ent.Sympath)})
	}
	return s.mds.GetChainINode(agro.NewINodeRef(vol, agro.INodeID(ent.Chain)))
}

type FileInfo struct {
	Path agro.Path

	// And one of
	Ref   agro.INodeRef
	INode *models.INode

	Dir *models.Directory

	Symlink string
}

func (fi FileInfo) Name() string {
	return fi.Path.Path
}

func (fi FileInfo) Size() int64 {
	if fi.IsDir() {
		return int64(len(fi.Dir.Files))
	}
	if fi.Symlink != "" {
		return 0
	}
	return int64(fi.INode.Filesize)
}

func (fi FileInfo) Mode() os.FileMode {
	if fi.IsDir() {
		return os.FileMode(fi.Dir.Metadata.Mode)
	}
	if fi.Symlink != "" {
		return 0777 | os.ModeSymlink
	}
	return os.FileMode(fi.INode.Permissions.Mode)
}

func (fi FileInfo) ModTime() time.Time {
	if fi.IsDir() {
		return time.Unix(0, int64(fi.Dir.Metadata.Mtime))
	}
	if fi.Symlink != "" {
		return time.Unix(0, 0)
	}
	return time.Unix(0, int64(fi.INode.Permissions.Mtime))
}

func (fi FileInfo) IsDir() bool {
	return fi.Path.IsDir()
}

func (fi FileInfo) Sys() interface{} {
	return fi
}

func (s *server) Lstat(path agro.Path) (os.FileInfo, error) {
	promOps.WithLabelValues("lstat").Inc()
	s.mut.RLock()
	defer s.mut.RUnlock()
	for _, v := range s.openFileChains {
		for _, x := range v.fh.inode.Filenames {
			if path.Path == x {
				return FileInfo{
					INode: v.fh.inode,
					Path:  path,
					Ref:   agro.NewINodeRef(agro.VolumeID(v.fh.inode.Volume), agro.INodeID(v.fh.inode.INode)),
				}, nil
			}
		}
	}
	clog.Tracef("lstat %s", path)
	if path.IsDir() {
		clog.Tracef("is dir")
		d, _, err := s.mds.Getdir(path)
		return FileInfo{
			Path: path,
			Dir:  d,
		}, err
	}
	vol, ent, err := s.FileEntryForPath(path)
	if err != nil {
		return nil, err
	}
	if ent.Sympath != "" {
		return FileInfo{
			Path:    path,
			Ref:     agro.NewINodeRef(vol, agro.INodeID(0)),
			Symlink: ent.Sympath,
		}, nil
	}
	ref, err := s.mds.GetChainINode(agro.NewINodeRef(vol, agro.INodeID(ent.Chain)))
	if err != nil {
		return nil, err
	}

	inode, err := s.inodes.GetINode(context.TODO(), ref)
	if err != nil {
		return nil, err
	}

	return FileInfo{
		INode: inode,
		Path:  path,
		Ref:   ref,
	}, nil
}

func (s *server) Readdir(path agro.Path) ([]agro.Path, error) {
	promOps.WithLabelValues("readdir").Inc()
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

func (s *server) Mkdir(path agro.Path, md *models.Metadata) error {
	promOps.WithLabelValues("mkdir").Inc()
	if !path.IsDir() {
		return os.ErrInvalid
	}
	return s.mds.Mkdir(path, md)
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

func (s *server) incRef(vol string, bm *roaring.Bitmap) {
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

func (s *server) decRef(vol string, bm *roaring.Bitmap) {
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

func (s *server) getBitmap(vol string) (*roaring.Bitmap, bool) {
	s.mut.Lock()
	defer s.mut.Unlock()
	if _, ok := s.openINodeRefs[vol]; !ok {
		return nil, false
	}
	out := roaring.NewBitmap()
	for k := range s.openINodeRefs[vol] {
		out.Add(uint32(k))
	}
	return out, true
}

func (s *server) Remove(path agro.Path) error {
	promOps.WithLabelValues("remove").Inc()
	if path.IsDir() {
		return s.removeDir(path)
	}
	return s.removeFile(path)
}

// TODO(barakmich): Split into two functions, one for chain ID and one for path.
func (s *server) updateINodeChain(ctx context.Context, p agro.Path, modFunc func(oldINode *models.INode, vol agro.VolumeID) (*models.INode, agro.INodeRef, error)) (*models.INode, agro.INodeRef, error) {
	notExist := false
	vol, entry, err := s.FileEntryForPath(p)
	clog.Tracef("vol: %v, entry, %v, err %s", vol, entry, err)
	ref := agro.NewINodeRef(vol, agro.INodeID(0))
	if err != nil {
		if err != os.ErrNotExist {
			return nil, ref, err
		}
		notExist = true
		entry = &models.FileEntry{}
	} else {
		if entry.Sympath != "" {
			return nil, ref, agro.ErrIsSymlink
		}
	}
	clog.Tracef("notexist: %v", notExist)
	chainRef := agro.NewINodeRef(vol, agro.INodeID(entry.Chain))
	for {
		var inode *models.INode
		if !notExist {
			ref, err = s.mds.GetChainINode(chainRef)
			clog.Tracef("ref: %s", ref)
			if err != nil {
				return nil, ref, err
			}
			inode, err = s.inodes.GetINode(ctx, ref)
			if err != nil {
				return nil, ref, err
			}
		}
		newINode, newRef, err := modFunc(inode, vol)
		if err != nil {
			return nil, ref, err
		}
		if chainRef.INode == 0 {
			err = s.mds.SetChainINode(newRef, chainRef, newRef)
		} else {
			err = s.mds.SetChainINode(chainRef, ref, newRef)
		}
		if err == nil {
			return newINode, ref, s.inodes.WriteINode(ctx, newRef, newINode)
		}
		if err == agro.ErrCompareFailed {
			continue
		}
		return nil, ref, err
	}
}

func (s *server) removeDir(path agro.Path) error {
	return s.mds.Rmdir(path)
}

func (s *server) addOpenFile(chainID uint64, fh *fileHandle) {
	s.mut.Lock()
	defer s.mut.Unlock()
	if v, ok := s.openFileChains[chainID]; ok {
		v.count++
		if fh != v.fh {
			panic("different pointers?")
		}
		s.openFileChains[chainID] = v
	} else {
		s.openFileChains[chainID] = openFileCount{fh, 1}
	}
	clog.Tracef("addOpenFile %#v", s.openFileChains)
}

func (s *server) getOpenFile(chainID uint64) (fh *fileHandle) {
	if v, ok := s.openFileChains[chainID]; ok {
		clog.Tracef("got open file %v", s.openFileChains)
		return v.fh
	}
	clog.Tracef("did not get open file")
	return nil
}

func (s *server) removeOpenFile(chainID uint64) {
	s.mut.Lock()
	v, ok := s.openFileChains[chainID]
	if !ok {
		panic("removing unopened handle")
	}
	v.count--
	if v.count != 0 {
		s.openFileChains[chainID] = v
		s.mut.Unlock()
		return
	}
	delete(s.openFileChains, chainID)
	s.mut.Unlock()
	v.fh.mut.Lock()
	defer v.fh.mut.Unlock()
	v.fh.updateHeldINodes(true)
	clog.Tracef("removeOpenFile %#v", s.openFileChains)
}

func (s *server) Statfs() (agro.ServerStats, error) {
	gmd, err := s.mds.GlobalMetadata()
	if err != nil {
		return agro.ServerStats{}, err
	}
	return agro.ServerStats{
		BlockSize: gmd.BlockSize,
	}, nil
}

func (s *server) Debug(w io.Writer) error {
	if v, ok := s.mds.(agro.DebugMetadataService); ok {
		io.WriteString(w, "# MDS\n")
		return v.DumpMetadata(w)
	}
	return nil
}

func (s *server) getContext() context.Context {
	wl := context.WithValue(context.TODO(), ctxWriteLevel, s.cfg.WriteLevel)
	rl := context.WithValue(wl, ctxReadLevel, s.cfg.ReadLevel)
	return rl
}
