package server

import (
	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
	"golang.org/x/net/context"
)

func getRepFromContext(ctx context.Context) int {
	if ctx == nil {
		return 1
	}
	rep := ctx.Value("replication")
	if rep == nil {
		return 1
	}
	return rep.(int)
}

func (d *distributor) GetINode(ctx context.Context, i agro.INodeRef) (*models.INode, error) {
	rep := getRepFromContext(ctx)
	peers, err := d.ring.GetINodePeers(i, rep)
	if err != nil {
		return nil, err
	}
	if len(peers) == 0 {
		return nil, agro.ErrOutOfSpace
	}
	// TODO(barakmich): Retry other peers
	if peers[0] == d.srv.mds.UUID() {
		return d.inodes.GetINode(ctx, i)
	}
	return d.client.GetINode(ctx, peers[0], i)
}

func (d *distributor) WriteINode(ctx context.Context, i agro.INodeRef, inode *models.INode) error {
	rep := getRepFromContext(ctx)
	peers, err := d.ring.GetINodePeers(i, rep)
	if err != nil {
		return err
	}
	if len(peers) == 0 {
		return agro.ErrOutOfSpace
	}
	for _, p := range peers {
		var err error
		if p == d.srv.mds.UUID() {
			err = d.inodes.WriteINode(ctx, i, inode)
		} else {
			err = d.client.PutINode(ctx, p, i, inode, rep)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *distributor) DeleteINode(ctx context.Context, i agro.INodeRef) error {
	return d.inodes.DeleteINode(ctx, i)
}

func (d *distributor) GetBlock(ctx context.Context, i agro.BlockRef) ([]byte, error) {
	rep := getRepFromContext(ctx)
	peers, err := d.ring.GetBlockPeers(i, rep)
	if err != nil {
		return nil, err
	}
	if len(peers) == 0 {
		return nil, agro.ErrOutOfSpace
	}
	// TODO(barakmich): Retry other peers
	if peers[0] == d.srv.mds.UUID() {
		return d.blocks.GetBlock(ctx, i)
	}
	return d.client.GetBlock(ctx, peers[0], i)
}

func (d *distributor) WriteBlock(ctx context.Context, i agro.BlockRef, data []byte) error {
	rep := getRepFromContext(ctx)
	peers, err := d.ring.GetBlockPeers(i, rep)
	if err != nil {
		return err
	}
	if len(peers) == 0 {
		return agro.ErrOutOfSpace
	}
	for _, p := range peers {
		var err error
		if p == d.srv.mds.UUID() {
			err = d.blocks.WriteBlock(ctx, i, data)
		} else {
			err = d.client.PutBlock(ctx, p, i, data, rep)
		}
		if err != nil {
			return err
		}
	}
	return nil
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
