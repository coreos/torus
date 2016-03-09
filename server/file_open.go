package server

import (
	"errors"
	"os"

	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/models"
	"github.com/coreos/pkg/capnslog"
)

var (
	mlog = capnslog.NewPackageLogger("github.com/coreos/agro", "map")
)

func (s *server) Create(path agro.Path) (agro.File, error) {
	return s.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

func (s *server) create(path agro.Path, flag int, md *models.Metadata) (f agro.File, err error) {
	// Truncate the file if it already exists. This is equivalent to creating
	// a new (empty) inode with the path that we're going to overwrite later.
	rdOnly, wrOnly, err := onlys(flag)
	if err != nil {
		return nil, err
	}
	n := models.NewEmptyINode()
	n.Filenames = []string{path.Path}
	_, ent, err := s.FileEntryForPath(path)
	if err != nil {
		if err != os.ErrNotExist {
			return nil, err
		}
		ent = &models.FileEntry{}
	}
	n.Chain = ent.Chain
	vol, err := s.mds.GetVolume(path.Volume)
	n.Volume = vol.Id
	if err != nil {
		return nil, err
	}
	n.Permissions = md
	globals, err := s.mds.GlobalMetadata()
	if err != nil {
		return nil, err
	}
	bs, err := blockset.CreateBlocksetFromSpec(globals.DefaultBlockSpec, s.blocks)
	if err != nil {
		return nil, err
	}
	clog.Tracef("Create file %s at inode %d:%d with block length %d", path, n.Volume, n.INode, bs.Length())
	fh := &fileHandle{
		volume:        vol,
		inode:         n,
		srv:           s,
		blocks:        bs,
		blkSize:       int64(globals.BlockSize),
		initialINodes: roaring.NewBitmap(),
		changed:       make(map[string]bool),
	}
	file := &file{
		fileHandle: fh,
		path:       path,
		readOnly:   rdOnly,
		writeOnly:  wrOnly,
	}
	// Fake a write, to open up the created file
	written, err := file.Write([]byte{})
	if written != 0 || err != nil {
		return nil, errors.New("couldn't write empty data to the file")
	}
	file.Sync()
	s.addOpenFile(fh.inode.Chain, fh)
	return file, nil
}

func (s *server) Open(p agro.Path) (agro.File, error) {
	return s.OpenFile(p, os.O_RDONLY, 0)
}

func (s *server) OpenFile(p agro.Path, flag int, perm os.FileMode) (agro.File, error) {
	clog.Debugf("opening file %s", p)
	return s.openFile(p, flag, &models.Metadata{
		Mode: uint32(perm),
	})
}

func (s *server) OpenFileMetadata(p agro.Path, flag int, md *models.Metadata) (agro.File, error) {
	return s.openFile(p, flag, md)
}

func (s *server) openFile(p agro.Path, flag int, md *models.Metadata) (agro.File, error) {
	vol, err := s.mds.GetVolume(p.Volume)
	if vol.Type != models.Volume_FILE {
		return nil, agro.ErrWrongVolumeType
	}
	if flag&os.O_CREATE != 0 && (flag&os.O_EXCL) == 0 {
		return s.create(p, flag, md)
	}
	ref, err := s.inodeRefForPath(p)
	if (flag&os.O_CREATE) != 0 && flag&os.O_EXCL != 0 {
		perr, ok := err.(*os.PathError)
		if err == nil || (ok && perr.Err != os.ErrNotExist) {
			return nil, os.ErrExist
		}
		return s.create(p, flag, md)
	}
	if err != nil {
		return nil, err
	}

	inode, err := s.inodes.GetINode(s.getContext(), ref)
	if err != nil {
		return nil, err
	}

	fh := s.getOpenFile(inode.Chain)
	if fh != nil {
		s.addOpenFile(inode.Chain, fh)
		rdOnly, wrOnly, err := onlys(flag)
		if err != nil {
			return nil, err
		}
		return &file{
			fileHandle: fh,
			path:       p,
			readOnly:   rdOnly,
			writeOnly:  wrOnly,
		}, nil
	}

	// TODO(jzelinskie): check metadata for permission
	return s.newFile(p, flag, inode)

}

func onlys(flag int) (rdOnly bool, wrOnly bool, err error) {
	modeMask := os.O_WRONLY | os.O_RDWR | os.O_RDONLY
	switch flag & modeMask {
	case os.O_WRONLY:
		return false, true, nil
	case os.O_RDONLY:
		return true, false, nil
	case os.O_RDWR:
		return false, false, nil
	default:
		return false, false, os.ErrInvalid
	}
}

func (s *server) newFile(path agro.Path, flag int, inode *models.INode) (agro.File, error) {
	rdOnly, wrOnly, err := onlys(flag)
	if err != nil {
		return nil, err
	}
	bs, err := blockset.UnmarshalFromProto(inode.GetBlocks(), s.blocks)
	if err != nil {
		return nil, err
	}
	md, err := s.mds.GlobalMetadata()
	if err != nil {
		return nil, err
	}
	vol, err := s.mds.GetVolume(path.Volume)
	if err != nil {
		return nil, err
	}

	set := bs.GetLiveINodes()
	s.incRef(path.Volume, set)
	bm, ok := s.getBitmap(path.Volume)
	mlog.Tracef("updating claim %s %s", path.Volume, bm)
	err = s.fsMDS().ClaimVolumeINodes(s.lease, agro.VolumeID(vol.Id), bm)
	if err != nil {
		s.decRef(path.Volume, set)
		return nil, err
	}
	if ok {
		promOpenINodes.WithLabelValues(path.Volume).Set(float64(bm.GetCardinality()))
	} else {
		promOpenINodes.WithLabelValues(path.Volume).Set(0)
	}

	clog.Debugf("Open file %s at inode %d:%d with block length %d and size %d", path, inode.Volume, inode.INode, bs.Length(), inode.Filesize)
	fh := &fileHandle{
		volume:        vol,
		inode:         inode,
		srv:           s,
		blocks:        bs,
		initialINodes: set,
		blkSize:       int64(md.BlockSize),
		changed:       make(map[string]bool),
	}
	f := &file{
		fileHandle: fh,
		path:       path,
		readOnly:   rdOnly,
		writeOnly:  wrOnly,
	}
	s.addOpenFile(inode.Chain, fh)
	if flag&os.O_TRUNC != 0 {
		f.Truncate(0)
	}
	if flag&os.O_APPEND != 0 {
		f.offset = int64(inode.Filesize)
	}
	promOpenFiles.WithLabelValues(path.Volume).Inc()
	return f, nil
}
