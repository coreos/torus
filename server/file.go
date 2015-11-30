package server

import (
	"errors"
	"io"
	"sync"

	"github.com/coreos/pkg/capnslog"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/blockset"
	"github.com/barakmich/agro/models"
)

var clog = capnslog.NewPackageLogger("github.com/barakmich/agro", "server")

type file struct {
	mut       sync.RWMutex
	path      agro.Path
	inode     *models.INode
	srv       *server
	offset    int64
	blocks    agro.Blockset
	blkSize   int64
	inodeRef  agro.INodeRef
	writeOpen bool
	flags     int
}

func (s *server) newFile(path agro.Path, inode *models.INode) (agro.File, error) {
	bs, err := blockset.UnmarshalFromProto(inode.GetBlocks(), s.cold)
	if err != nil {
		return nil, err
	}
	md, err := s.mds.GlobalMetadata()
	if err != nil {
		return nil, err
	}

	clog.Tracef("Open file %s at inode %d:%d with block length %d and size %d", path, inode.Volume, inode.Inode, bs.Length(), inode.Filesize)
	f := &file{
		path:    path,
		inode:   inode,
		srv:     s,
		offset:  0,
		blocks:  bs,
		blkSize: int64(md.BlockSize),
	}
	return f, nil
}

func (f *file) Write(b []byte) (n int, err error) {
	n, err = f.WriteAt(b, f.offset)
	f.offset += int64(n)
	return
}

func (f *file) openWrite() error {
	if f.writeOpen {
		return nil
	}
	vid, err := f.srv.mds.GetVolumeID(f.path.Volume)
	if err != nil {
		return err
	}
	newInode, err := f.srv.mds.CommitInodeIndex()
	if err != nil {
		if err == agro.ErrAgain {
			return f.openWrite()
		}
		return err
	}
	f.inodeRef = agro.INodeRef{
		Volume: vid,
		INode:  newInode,
	}
	if f.inode != nil {
		f.inode.Replaces = f.inode.Inode
		f.inode.Inode = uint64(newInode)
	}
	f.writeOpen = true
	return nil
}

func (f *file) WriteAt(b []byte, off int64) (n int, err error) {
	f.mut.Lock()
	defer f.mut.Unlock()
	clog.Trace("begin write: offset ", off, " size ", len(b))
	toWrite := len(b)
	err = f.openWrite()
	if err != nil {
		return 0, err
	}

	defer func() {
		if off > int64(f.inode.Filesize) {
			f.inode.Filesize = uint64(off)
		}
	}()

	// Write the front matter, which may dangle from a byte offset
	blkIndex := int(off / f.blkSize)

	if f.blocks.Length() > blkIndex {
		// TODO(barakmich) Support truncate in the block abstraction, fill/return 0s
		return n, errors.New("Can't write past the end of a file")
	}

	blkOff := off - int64(int(f.blkSize)*blkIndex)
	if blkOff != 0 {
		frontlen := int(f.blkSize - blkOff)
		if frontlen > toWrite {
			frontlen = toWrite
		}
		var blk []byte
		if f.blocks.Length() == blkIndex {
			blk = make([]byte, f.blkSize)
		} else {
			blk, err = f.blocks.GetBlock(nil, blkIndex)
			if err != nil {
				return n, err
			}
		}
		wrote := copy(blk[int(blkOff):int(blkOff)+frontlen], b[:frontlen])
		clog.Tracef("head writing block at index %d, inoderef %s", blkIndex, f.inodeRef)
		err = f.blocks.PutBlock(nil, f.inodeRef, blkIndex, blk)
		if err != nil {
			return n, err
		}
		if wrote != frontlen {
			return n, errors.New("Couldn't write all of the first block at the offset")
		}
		b = b[frontlen:]
		n += wrote
		off += int64(wrote)
	}

	toWrite = len(b)
	if toWrite == 0 {
		// We're done
		return n, nil
	}

	// Bulk Write! We'd rather be here.
	if off%f.blkSize != 0 {
		panic("Offset not equal to a block boundary")
	}

	for toWrite >= int(f.blkSize) {
		blkIndex := int(off / f.blkSize)
		clog.Tracef("bulk writing block at index %d, inoderef %s", blkIndex, f.inodeRef)
		err = f.blocks.PutBlock(nil, f.inodeRef, blkIndex, b[:f.blkSize])
		if err != nil {
			return n, err
		}
		b = b[f.blkSize:]
		n += int(f.blkSize)
		off += int64(f.blkSize)
		toWrite = len(b)
	}

	if toWrite == 0 {
		// We're done
		return n, nil
	}

	// Trailing matter. This sucks too.
	if off%f.blkSize != 0 {
		panic("Offset not equal to a block boundary after bulk")
	}
	blkIndex = int(off / f.blkSize)
	var blk []byte
	if f.blocks.Length() == blkIndex {
		blk = make([]byte, f.blkSize)
	} else {
		blk, err = f.blocks.GetBlock(nil, blkIndex)
		if err != nil {
			return n, err
		}
	}
	wrote := copy(blk[:toWrite], b)
	clog.Tracef("tail writing block at index %d, inoderef %s", blkIndex, f.inodeRef)
	err = f.blocks.PutBlock(nil, f.inodeRef, blkIndex, blk)
	if err != nil {
		return n, err
	}
	if wrote != toWrite {
		return n, errors.New("Couldn't write all of the last block")
	}
	b = b[wrote:]
	n += wrote
	off += int64(wrote)
	return n, nil
}

func (f *file) Read(b []byte) (n int, err error) {
	n, err = f.ReadAt(b, f.offset)
	f.offset += int64(n)
	return
}

func (f *file) ReadAt(b []byte, off int64) (n int, ferr error) {
	f.mut.RLock()
	defer f.mut.RUnlock()
	toRead := len(b)
	clog.Tracef("begin read of size %d", toRead)
	n = 0
	if int64(toRead)+off > int64(f.inode.Filesize) {
		toRead = int(int64(f.inode.Filesize) - off)
		ferr = io.EOF
		clog.Tracef("read is longer than file")
	}
	for toRead > n {
		blkIndex := int(off / f.blkSize)
		if f.blocks.Length() <= blkIndex {
			// TODO(barakmich) Support truncate in the block abstraction, fill/return 0s
			return n, io.EOF
		}
		blkOff := off - int64(int(f.blkSize)*blkIndex)
		clog.Tracef("getting block index %d", blkIndex)
		blk, err := f.blocks.GetBlock(nil, blkIndex)
		if err != nil {
			return n, err
		}
		thisRead := f.blkSize - blkOff
		if int64(toRead-n) < thisRead {
			thisRead = int64(toRead - n)
		}
		count := copy(b[n:], blk[blkOff:blkOff+thisRead])
		n += count
		off += int64(count)
	}
	if toRead != n {
		panic("Read more than n bytes?")
	}
	return n, ferr
}

func (f *file) Close() error {
	if f == nil {
		return agro.ErrInvalid
	}
	return f.Sync()
}

func (f *file) Sync() error {
	if !f.writeOpen {
		return nil
	}
	blkdata, err := blockset.MarshalToProto(f.blocks)
	if err != nil {
		return err
	}
	f.inode.Blocks = blkdata
	err = f.srv.inodes.WriteINode(nil, f.inodeRef, f.inode)
	if err != nil {
		return err
	}

	err = f.srv.mds.SetFileINode(f.path, f.inodeRef)
	if err != nil {
		return err
	}
	return nil
}
