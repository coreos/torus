package server

import (
	"errors"
	"io"
	"sync"

	"golang.org/x/net/context"

	"github.com/coreos/pkg/capnslog"

	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/models"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "server")

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
	openIdx   int
	openData  []byte
}

func (s *server) newFile(path agro.Path, inode *models.INode) (agro.File, error) {
	bs, err := blockset.UnmarshalFromProto(inode.GetBlocks(), s.blocks)
	if err != nil {
		return nil, err
	}
	md, err := s.mds.GlobalMetadata()
	if err != nil {
		return nil, err
	}

	clog.Tracef("Open file %s at inode %d:%d with block length %d and size %d", path, inode.Volume, inode.INode, bs.Length(), inode.Filesize)
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
	newInode, err := f.srv.mds.CommitInodeIndex(f.path.Volume)
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
		f.inode.Replaces = f.inode.INode
		f.inode.INode = uint64(newInode)
	}
	f.writeOpen = true
	return nil
}

func (f *file) openBlock(i int) error {
	if f.openIdx == i && f.openData != nil {
		return nil
	}
	if f.openData != nil {
		err := f.syncBlock()
		if err != nil {
			return err
		}
	}
	if f.blocks.Length() == i {
		f.openData = make([]byte, f.blkSize)
		f.openIdx = i
		return nil
	}
	d, err := f.blocks.GetBlock(context.TODO(), i)
	if err != nil {
		return err
	}
	f.openData = d
	f.openIdx = i
	return nil
}

func (f *file) writeToBlock(from, to int, data []byte) int {
	if f.openData == nil {
		panic("server: file data not open")
	}
	if (to - from) != len(data) {
		panic("server: different write lengths?")
	}
	return copy(f.openData[from:to], data)
}

func (f *file) syncBlock() error {
	if f.openData == nil {
		return nil
	}
	err := f.blocks.PutBlock(context.TODO(), f.inodeRef, f.openIdx, f.openData)
	f.openIdx = -1
	f.openData = nil
	return err
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

	if f.blocks.Length() < blkIndex {
		// TODO(barakmich) Support truncate in the block abstraction, fill/return 0s
		return n, errors.New("Can't write past the end of a file")
	}

	blkOff := off - int64(int(f.blkSize)*blkIndex)
	if blkOff != 0 {
		frontlen := int(f.blkSize - blkOff)
		if frontlen > toWrite {
			frontlen = toWrite
		}
		err := f.openBlock(blkIndex)
		if err != nil {
			return n, err
		}
		wrote := f.writeToBlock(int(blkOff), int(blkOff)+frontlen, b[:frontlen])
		clog.Tracef("head writing block at index %d, inoderef %s", blkIndex, f.inodeRef)
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
		err = f.blocks.PutBlock(context.TODO(), f.inodeRef, blkIndex, b[:f.blkSize])
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
	err = f.openBlock(blkIndex)
	if err != nil {
		return n, err
	}
	wrote := f.writeToBlock(0, toWrite, b)
	clog.Tracef("tail writing block at index %d, inoderef %s", blkIndex, f.inodeRef)
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
		blk, err := f.blocks.GetBlock(context.TODO(), blkIndex)
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
	err := f.Sync()
	if err != nil {
		clog.Error(err)
	}
	return err
}

func (f *file) Sync() error {
	if !f.writeOpen {
		return nil
	}
	err := f.syncBlock()
	if err != nil {
		clog.Error("sync: couldn't sync block")
		return err
	}
	blkdata, err := blockset.MarshalToProto(f.blocks)
	if err != nil {
		clog.Error("sync: couldn't marshal proto")
		return err
	}
	f.inode.Blocks = blkdata
	err = f.srv.inodes.WriteINode(context.TODO(), f.inodeRef, f.inode)
	if err != nil {
		clog.Error("sync: couldn't write inode")
		return err
	}

	err = f.srv.mds.SetFileINode(f.path, f.inodeRef)
	if err != nil {
		clog.Error("sync: couldn't set file inode")
		return err
	}
	return nil
}
