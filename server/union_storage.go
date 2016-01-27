package server

import (
	"github.com/coreos/agro"
	"golang.org/x/net/context"
)

type unionStorage struct {
	oldBlock agro.BlockStore
	newBlock agro.BlockStore
}

func (r *unionStorage) GetBlock(ctx context.Context, i agro.BlockRef) ([]byte, error) {
	return r.oldBlock.GetBlock(ctx, i)
}

func (r *unionStorage) WriteBlock(ctx context.Context, i agro.BlockRef, data []byte) error {
	err := r.oldBlock.WriteBlock(ctx, i, data)
	if err != nil {
		return err
	}
	return r.newBlock.WriteBlock(ctx, i, data)
}

func (r *unionStorage) DeleteBlock(ctx context.Context, i agro.BlockRef) error {
	err := r.oldBlock.DeleteBlock(ctx, i)
	if err != nil {
		return err
	}
	return r.newBlock.DeleteBlock(ctx, i)
}

func (r *unionStorage) DeleteINodeBlocks(ctx context.Context, i agro.INodeRef) error {
	err := r.oldBlock.DeleteINodeBlocks(ctx, i)
	if err != nil {
		return err
	}
	return r.newBlock.DeleteINodeBlocks(ctx, i)
}

func (r *unionStorage) NumBlocks() uint64 {
	return r.oldBlock.NumBlocks()
}

func (r *unionStorage) UsedBlocks() uint64 {
	return r.oldBlock.UsedBlocks()
}

func (r *unionStorage) BlockIterator() agro.BlockIterator {
	return r.oldBlock.BlockIterator()
}

func (r *unionStorage) Flush() error {
	r.oldBlock.Flush()
	return r.newBlock.Flush()
}

func (r *unionStorage) Close() error {
	r.oldBlock.Close()
	return r.newBlock.Close()
}

func (r *unionStorage) BlockSize() uint64 {
	return r.newBlock.BlockSize()
}

func (r *unionStorage) Kind() string { return "union" }

func (r *unionStorage) ReplaceBlockStore(bs agro.BlockStore) error {
	panic("unimplemented")
}
