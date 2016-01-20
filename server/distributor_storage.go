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
	d.mut.RLock()
	defer d.mut.RUnlock()
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
			in, err := d.inodes.GetINode(ctx, i)
			if err == nil {
				return in, nil
			}
			break
		}
	}
	for _, p := range peers {
		if p == d.UUID() {
			continue
		}
		in, err := d.client.GetINode(ctx, p, i)
		if err == nil {
			return in, nil
		}
		if err == agro.ErrINodeUnavailable {
			clog.Debug("inode failed, trying next peer")
			continue
		}
		return nil, err
	}
	return nil, ErrNoPeersINode
}

func (d *distributor) WriteINode(ctx context.Context, i agro.INodeRef, inode *models.INode) error {
	d.mut.RLock()
	defer d.mut.RUnlock()
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

func (d *distributor) INodeIterator() agro.INodeIterator {
	return d.inodes.INodeIterator()
}

func (d *distributor) GetBlock(ctx context.Context, i agro.BlockRef) ([]byte, error) {
	d.mut.RLock()
	defer d.mut.RUnlock()
	if d.readCache != nil {
		bcache, ok := d.readCache.Get(string(i.ToBytes()))
		if ok {
			return bcache.([]byte), nil
		}
	}
	peers, err := d.ring.GetBlockPeers(i)
	if err != nil {
		return nil, err
	}
	if len(peers) == 0 {
		return nil, ErrNoPeersBlock
	}
	for _, p := range peers {
		if p == d.UUID() {
			b, err := d.blocks.GetBlock(ctx, i)
			if err == nil {
				return b, nil
			}
			break
		}
	}
	for _, p := range peers {
		if p == d.UUID() {
			continue
		}
		blk, err := d.client.GetBlock(ctx, p, i)
		if err == nil {
			if d.readCache != nil {
				d.readCache.Add(string(i.ToBytes()), blk)
			}
			return blk, nil
		}
		if err == agro.ErrBlockUnavailable {
			clog.Debug("block failed, trying next peer")
			continue
		}
		return nil, err
	}
	return nil, ErrNoPeersBlock
}

func (d *distributor) WriteBlock(ctx context.Context, i agro.BlockRef, data []byte) error {
	d.mut.RLock()
	defer d.mut.RUnlock()
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
	d.mut.RLock()
	defer d.mut.RUnlock()
	return d.blocks.NumBlocks()
}

func (d *distributor) UsedBlocks() uint64 {
	d.mut.RLock()
	defer d.mut.RUnlock()
	return d.blocks.UsedBlocks()
}

func (d *distributor) BlockIterator() agro.BlockIterator {
	return d.blocks.BlockIterator()
}

func (d *distributor) Flush() error {
	return d.blocks.Flush()
}

func (d *distributor) Kind() string { return "distributor" }

func (d *distributor) ReplaceINodeStore(is agro.INodeStore) (agro.INodeStore, error) {
	return d.inodes.ReplaceINodeStore(is)
}

func (d *distributor) ReplaceBlockStore(bs agro.BlockStore) (agro.BlockStore, error) {
	return d.blocks.ReplaceBlockStore(bs)
}
