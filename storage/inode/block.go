package inode

import (
	"encoding/binary"
	"errors"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"golang.org/x/net/context"
)

var _ agro.INodeStore = &blockINodeStore{}

type blockINodeStore struct {
	bs        agro.BlockStore
	name      string
	blocksize uint64
}

func NewBlockINodeStore(name string, cfg agro.Config, bs agro.BlockStore, gmd agro.GlobalMetadata) (agro.INodeStore, error) {
	return &blockINodeStore{
		bs:        bs,
		name:      name,
		blocksize: gmd.BlockSize,
	}, nil
}

func (b *blockINodeStore) Kind() string { return "block" }
func (b *blockINodeStore) Flush() error { return b.bs.Flush() }
func (b *blockINodeStore) Close() error {
	return b.bs.Close()
}

func (b *blockINodeStore) WriteINode(ctx context.Context, i agro.INodeRef, inode *models.INode) error {
	inodedata, err := inode.Marshal()
	if err != nil {
		return err
	}
	buf := make([]byte, b.blocksize)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(inodedata)))
	bufoffset := 4
	inodeoffset := 0
	index := 1
	for inodeoffset != len(inodedata) {
		if bufoffset == 0 {
			buf = make([]byte, b.blocksize)
		}
		written := copy(buf[bufoffset:], inodedata[inodeoffset:])
		inodeoffset += written
		ref := agro.BlockRef{
			INodeRef: i,
			Index:    agro.IndexID(index),
		}
		ref.SetBlockType(agro.INode)
		err := b.bs.WriteBlock(ctx, ref, buf)
		if err != nil {
			return err
		}
		bufoffset = 0
		index++
	}
	return nil
}

func (b *blockINodeStore) GetINode(ctx context.Context, i agro.INodeRef) (*models.INode, error) {
	index := 1
	ref := agro.BlockRef{
		INodeRef: i,
		Index:    agro.IndexID(index),
	}
	ref.SetBlockType(agro.INode)
	data, err := b.bs.GetBlock(ctx, ref)
	if err != nil {
		return nil, err
	}
	dlen := binary.LittleEndian.Uint32(data[0:4])
	buf := make([]byte, dlen)
	bufoffset := 0
	dataoffset := 4
	for bufoffset != int(dlen) {
		if dataoffset == 0 {
			index++
			ref := agro.BlockRef{
				INodeRef: i,
				Index:    agro.IndexID(index),
			}
			ref.SetBlockType(agro.INode)
			data, err = b.bs.GetBlock(ctx, ref)
			if err != nil {
				return nil, err
			}
		}
		written := copy(buf[bufoffset:], data[dataoffset:])
		dataoffset = 0
		bufoffset += written
	}
	out := &models.INode{}
	err = out.Unmarshal(buf)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (b *blockINodeStore) DeleteINode(ctx context.Context, i agro.INodeRef) error {
	ref := agro.BlockRef{
		INodeRef: i,
		Index:    agro.IndexID(1),
	}
	ref.SetBlockType(agro.INode)
	data, err := b.bs.GetBlock(ctx, ref)
	if err != nil {
		return err
	}
	dlen := binary.LittleEndian.Uint32(data[0:4])
	nblocks := (uint64(dlen) / b.blocksize) + 1
	for j := uint64(1); j <= nblocks; j++ {
		ref := agro.BlockRef{
			INodeRef: i,
			Index:    agro.IndexID(j),
		}
		ref.SetBlockType(agro.INode)
		err := b.bs.DeleteBlock(ctx, ref)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *blockINodeStore) ReplaceINodeStore(is agro.INodeStore) (agro.INodeStore, error) {
	if v, ok := is.(*blockINodeStore); ok {
		return &blockINodeStore{
			bs:        v.bs,
			name:      b.name,
			blocksize: v.blocksize,
		}, nil
	}
	return nil, errors.New("not a blockINodeStore")
}

func (b *blockINodeStore) INodeIterator() agro.INodeIterator {
	it := b.bs.BlockIterator()
	return &blockINodeIterator{it}
}

type blockINodeIterator struct {
	it agro.BlockIterator
}

func (i *blockINodeIterator) Err() error { return i.it.Err() }
func (i *blockINodeIterator) Next() bool {
	for i.it.Next() {
		ref := i.it.BlockRef()
		if ref.BlockType() == agro.INode && ref.Index == 1 {
			return true
		}
	}
	return false
}

func (i *blockINodeIterator) INodeRef() agro.INodeRef {
	return i.it.BlockRef().INodeRef
}

func (i *blockINodeIterator) Close() error {
	return i.it.Close()
}
