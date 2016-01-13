package server

import (
	"errors"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"golang.org/x/net/context"
)

var (
	ErrNoPeersBlock = errors.New("distributor: no peers available for a block")
	ErrNoPeersINode = errors.New("distributor: no peers available for an inode")
)

func getRepFromContext(ctx context.Context) int {
	if ctx == nil {
		return 1
	}
	rep := ctx.Value("replication")
	if rep == nil {
		return 1
	}
	repInt, ok := rep.(int)
	if !ok {
		clog.Fatalf("Cannot convert context value of type %#v to int", rep)
	}
	return repInt
}

func (d *distributor) GetINode(ctx context.Context, i agro.INodeRef) (*models.INode, error) {
	peers, err := d.ring.GetINodePeers(i)
	if err != nil {
		return nil, err
	}
	if len(peers) == 0 {
		return nil, ErrNoPeersINode
	}
	// Return it if we have it locally.
	for _, p := range peers {
		if p == d.UUID() {
			return d.inodes.GetINode(ctx, i)
		}
	}
	for _, p := range peers {
		in, err := d.client.GetINode(ctx, p, i)
		if err == nil {
			return in, nil
		}
		if err == agro.ErrINodeUnavailable {
			continue
		}
		return nil, err
	}
	return nil, ErrNoPeersINode
}

func (d *distributor) WriteINode(ctx context.Context, i agro.INodeRef, inode *models.INode) error {
	peers, err := d.ring.GetINodePeers(i)
	if err != nil {
		return err
	}
	if len(peers) == 0 {
		return ErrNoPeersINode
	}
	for _, p := range peers {
		var err error
		if p == d.srv.mds.UUID() {
			err = d.inodes.WriteINode(ctx, i, inode)
		} else {
			err = d.client.PutINode(ctx, p, i, inode)
		}
		if err != nil {
			// TODO(barakmich): It's the job of a downed peer to catch up.
			return err
		}
	}
	return nil
}

func (d *distributor) DeleteINode(ctx context.Context, i agro.INodeRef) error {
	return d.inodes.DeleteINode(ctx, i)
}
func (d *distributor) GetBlock(ctx context.Context, i agro.BlockRef) ([]byte, error) {
	peers, err := d.ring.GetBlockPeers(i)
	if err != nil {
		return nil, err
	}
	if len(peers) == 0 {
		return nil, ErrNoPeersBlock
	}
	for _, p := range peers {
		if p == d.UUID() {
			return d.blocks.GetBlock(ctx, i)
		}
	}
	for _, p := range peers {
		blk, err := d.client.GetBlock(ctx, p, i)
		if err == nil {
			return blk, nil
		}
		if err == agro.ErrBlockUnavailable {
			continue
		}
		return nil, err
	}
	return nil, ErrNoPeersBlock
}

func (d *distributor) WriteBlock(ctx context.Context, i agro.BlockRef, data []byte) error {
	peers, err := d.ring.GetBlockPeers(i)
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
			err = d.client.PutBlock(ctx, p, i, data)
		}
		if err != nil {
			// TODO(barakmich): It's the job of a downed peer to catch up.
			return err
		}
	}
	return nil
}

func (d *distributor) DeleteBlock(ctx context.Context, i agro.BlockRef) error {
	return d.blocks.DeleteBlock(ctx, i)
}

func (d *distributor) DeleteINodeBlocks(ctx context.Context, i agro.INodeRef) error {
	return d.blocks.DeleteINodeBlocks(ctx, i)
}

func (d *distributor) NumBlocks() uint64 {
	return d.blocks.NumBlocks()
}

func (d *distributor) UsedBlocks() uint64 {
	return d.blocks.UsedBlocks()
}

func (d *distributor) Flush() error {
	return d.blocks.Flush()
}
