package server

import (
	"errors"
	"io"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/blockset"
	"github.com/barakmich/agro/models"
)

type file struct {
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
	return &file{
		path:    path,
		inode:   inode,
		srv:     s,
		offset:  0,
		blocks:  bs,
		blkSize: int64(md.BlockSize),
	}, nil
}

func (f *file) Write(b []byte) (n int, err error) {
	return f.WriteAt(b, f.offset)
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
	toWrite := len(b)
	err = f.openWrite()
	if err != nil {
		return 0, err
	}

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
			blk, err = f.blocks.GetBlock(blkIndex)
			if err != nil {
				return n, err
			}
		}
		wrote := copy(b[:frontlen], blk[int(blkOff):int(blkOff)+frontlen])
		err = f.blocks.PutBlock(f.inodeRef, blkIndex, blk)
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
		f.offset += int64(n)
		return n, nil
	}

	// Bulk Write! We'd rather be here.
	if off%f.blkSize != 0 {
		panic("Offset not equal to a block boundary")
	}

	for toWrite > int(f.blkSize) {
		blkIndex := int(off / f.blkSize)
		err = f.blocks.PutBlock(f.inodeRef, blkIndex, b[:f.blkSize])
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
		f.offset += int64(n)
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
		blk, err = f.blocks.GetBlock(blkIndex)
		if err != nil {
			return n, err
		}
	}
	wrote := copy(b[:], blk[:toWrite])
	err = f.blocks.PutBlock(f.inodeRef, blkIndex, blk)
	if err != nil {
		return n, err
	}
	if wrote != toWrite {
		return n, errors.New("Couldn't write all of the last block")
	}
	b = b[wrote:]
	n += wrote
	off += int64(wrote)
	f.offset += int64(n)
	return n, nil
}

func (f *file) Read(b []byte) (n int, err error) {
	return 0, io.EOF
}
