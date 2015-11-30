package server

import (
	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
	"golang.org/x/net/context"
)

func (d *distributor) GetINode(ctx context.Context, i agro.INodeRef) (*models.INode, error) {
	return d.inodes.GetINode(ctx, i)
}

func (d *distributor) WriteINode(ctx context.Context, i agro.INodeRef, inode *models.INode) error {
	return d.inodes.WriteINode(ctx, i, inode)
}

func (d *distributor) DeleteINode(ctx context.Context, i agro.INodeRef) error {
	return d.inodes.DeleteINode(ctx, i)
}

func (d *distributor) GetBlock(ctx context.Context, i agro.BlockRef) ([]byte, error) {
	return d.blocks.GetBlock(ctx, i)
}

func (d *distributor) WriteBlock(ctx context.Context, i agro.BlockRef, data []byte) error {
	return d.blocks.WriteBlock(ctx, i, data)
}

func (d *distributor) DeleteBlock(ctx context.Context, i agro.BlockRef) error {
	return d.blocks.DeleteBlock(ctx, i)
}

func (d *distributor) NumBlocks() uint64 {
	return d.blocks.NumBlocks()
}

func (d *distributor) Flush() error {
	return d.blocks.Flush()
}
