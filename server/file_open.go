package server

import (
	"errors"
	"os"

	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/models"
	"github.com/RoaringBitmap/roaring"
	"golang.org/x/net/context"
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
	volid, err := s.mds.GetVolumeID(path.Volume)
	n.Volume = uint64(volid)
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
	file := &file{
		path:          path,
		inode:         n,
		srv:           s,
		blocks:        bs,
		blkSize:       int64(globals.BlockSize),
		initialINodes: roaring.NewBitmap(),
		readOnly:      rdOnly,
		writeOnly:     wrOnly,
		writeLevel:    agro.WriteOne,
	}
	s.addOpenFile(file)
	// Fake a write, to open up the created file
	written, err := file.Write([]byte{})
	if written != 0 || err != nil {
		return nil, errors.New("couldn't write empty data to the file")
	}
	return file, nil
}

func (s *server) Open(p agro.Path) (agro.File, error) {
	return s.OpenFile(p, os.O_RDONLY, 0)
}

func (s *server) OpenFile(p agro.Path, flag int, perm os.FileMode) (agro.File, error) {
	return s.openFile(p, flag, &models.Metadata{
		Mode: uint32(perm),
	})
}

func (s *server) OpenFileMetadata(p agro.Path, flag int, md *models.Metadata) (agro.File, error) {
	return s.openFile(p, flag, md)
}

func (s *server) openFile(p agro.Path, flag int, md *models.Metadata) (agro.File, error) {
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

	inode, err := s.inodes.GetINode(context.TODO(), ref)
	if err != nil {
		return nil, err
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

	set := bs.GetLiveINodes()
	s.incRef(path.Volume, set)
	bm, ok := s.getBitmap(path.Volume)
	err = s.mds.ClaimVolumeINodes(path.Volume, bm)
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
	f := &file{
		path:          path,
		inode:         inode,
		srv:           s,
		offset:        0,
		blocks:        bs,
		initialINodes: set,
		blkSize:       int64(md.BlockSize),
		readOnly:      rdOnly,
		writeOnly:     wrOnly,
		writeLevel:    agro.WriteOne,
	}
	s.addOpenFile(f)
	if flag&os.O_TRUNC != 0 {
		f.Truncate(0)
	}
	if flag&os.O_APPEND != 0 {
		f.offset = int64(inode.Filesize)
	}
	promOpenFiles.WithLabelValues(path.Volume).Inc()
	return f, nil
}
