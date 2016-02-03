package server

import (
	"errors"

	"github.com/coreos/agro"
	"golang.org/x/net/context"
)

var (
	ErrNoPeersBlock = errors.New("distributor: no peers available for a block")
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

func (d *distributor) GetBlock(ctx context.Context, i agro.BlockRef) ([]byte, error) {
	d.mut.RLock()
	defer d.mut.RUnlock()
	promDistBlockRequests.Inc()
	if d.readCache != nil {
		bcache, ok := d.readCache.Get(string(i.ToBytes()))
		if ok {
			promDistBlockCacheHits.Inc()
			return bcache.([]byte), nil
		}
	}
	peers, err := d.ring.GetPeers(i)
	if err != nil {
		promDistBlockFailures.Inc()
		return nil, err
	}
	if len(peers.Peers) == 0 {
		promDistBlockFailures.Inc()
		return nil, ErrNoPeersBlock
	}
	for _, p := range peers.Peers {
		if p == d.UUID() {
			b, err := d.blocks.GetBlock(ctx, i)
			if err == nil {
				promDistBlockLocalHits.Inc()
				return b, nil
			}
			promDistBlockLocalFailures.Inc()
			break
		}
	}
	for _, p := range peers.Peers {
		if p == d.UUID() {
			continue
		}
		blk, err := d.client.GetBlock(ctx, p, i)
		if err == nil {
			if d.readCache != nil {
				d.readCache.Add(string(i.ToBytes()), blk)
			}
			promDistBlockPeerHits.WithLabelValues(p).Inc()
			return blk, nil
		}
		if err == agro.ErrBlockUnavailable {
			clog.Debug("block failed, trying next peer")
			promDistBlockPeerFailures.WithLabelValues(p).Inc()
			continue
		}
		promDistBlockFailures.Inc()
		return nil, err
	}
	promDistBlockFailures.Inc()
	return nil, ErrNoPeersBlock
}

func (d *distributor) WriteBlock(ctx context.Context, i agro.BlockRef, data []byte) error {
	d.mut.RLock()
	defer d.mut.RUnlock()
	peers, err := d.ring.GetPeers(i)
	if err != nil {
		return err
	}
	if len(peers.Peers) == 0 {
		return agro.ErrOutOfSpace
	}
	for _, p := range peers.Peers {
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

func (d *distributor) BlockSize() uint64 {
	return d.blocks.BlockSize()
}
