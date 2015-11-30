package server

import (
	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
	"golang.org/x/net/context"
)

func (d *distributor) GetINode(ctx context.Context, i agro.INodeRef) (*models.INode, error) {
	rep, ok := ctx.Value("replication").(int)
	if !ok {
		rep = 1
	}
	peers, err := d.ring.GetINodePeers(i, rep)
	if err != nil {
		return nil, err
	}
	if len(peers) == 0 {
		return d.inodes.GetINode(ctx, i)
	}
	// TODO(barakmich): Retry other peers
	if peers[0] == d.srv.mds.UUID() {
		return d.inodes.GetINode(ctx, i)
	}
	pi := d.srv.peersMap[peers[0]]
	return d.client.GetINode(ctx, pi.Address, i)
}

func (d *distributor) WriteINode(ctx context.Context, i agro.INodeRef, inode *models.INode) error {
	return d.inodes.WriteINode(ctx, i, inode)
}

func (d *distributor) DeleteINode(ctx context.Context, i agro.INodeRef) error {
	return d.inodes.DeleteINode(ctx, i)
}

func (d *distributor) GetBlock(ctx context.Context, i agro.BlockRef) ([]byte, error) {
	rep, ok := ctx.Value("replication").(int)
	if !ok {
		rep = 1
	}
	peers, err := d.ring.GetBlockPeers(i, rep)
	if err != nil {
		return nil, err
	}
	if len(peers) == 0 {
		return d.blocks.GetBlock(ctx, i)
	}
	// TODO(barakmich): Retry other peers
	if peers[0] == d.srv.mds.UUID() {
		return d.blocks.GetBlock(ctx, i)
	}
	pi := d.srv.peersMap[peers[0]]
	return d.client.GetBlock(ctx, pi.Address, i)
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
