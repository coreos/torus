package server

import (
	"errors"

	"github.com/coreos/agro"
	"golang.org/x/net/context"
)

var (
	ErrNoPeersBlock = errors.New("distributor: no peers available for a block")
)

const (
	ctxWriteLevel int = iota
	ctxReadLevel
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
	if false {
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
	writeLevel := getWriteFromContext(ctx)
	for _, p := range peers.Peers[:peers.Replication] {
		if p == d.UUID() || writeLevel == agro.WriteLocal {
			b, err := d.blocks.GetBlock(ctx, i)
			if err == nil {
				promDistBlockLocalHits.Inc()
				return b, nil
			}
			promDistBlockLocalFailures.Inc()
			break
		}
	}
	quick := true
	for {
		for _, p := range peers.Peers {
			// If it's local, just try to get it.
			if p == d.UUID() {
				b, err := d.blocks.GetBlock(ctx, i)
				if err == nil {
					promDistBlockLocalHits.Inc()
					return b, nil
				}
				promDistBlockLocalFailures.Inc()
				clog.Debug("failed local peer (again)")
				continue
			}
			// Fetch block from remote. First pass through peers
			// with a timeout, then through the list without.
			var getctx context.Context
			var cancel context.CancelFunc
			if quick {
				getctx, cancel = context.WithTimeout(ctx, clientTimeout)
			} else {
				getctx = context.TODO()
			}
			blk, err := d.client.GetBlock(getctx, p, i)
			if quick {
				cancel()
			}

			// If we're successful, store that.
			if err == nil {
				if false {
					d.readCache.Put(string(i.ToBytes()), blk)
				}
				promDistBlockPeerHits.WithLabelValues(p).Inc()
				return blk, nil
			}

			// If this peer didn't have it, continue
			if err == agro.ErrBlockUnavailable {
				clog.Warningf("block from %s failed, trying next peer", p)
				promDistBlockPeerFailures.WithLabelValues(p).Inc()
				continue
			}

			// If there was a more significant error, fail hard.
			promDistBlockFailures.Inc()
			clog.Errorf("failed remote peer %v %s %s %#v", quick, p, err, err)
			return nil, err
		}
		// Well, we couldn't find it with timeouts, but perhaps all timeouts failed.
		// Let's go back through the list, without timeouts, and if we can't hit it
		// then we know we've failed.
		if !quick {
			break
		}
		clog.Warningf("trying slow method %s", i)
		quick = false
	}
	// We completely failed!
	promDistBlockFailures.Inc()
	clog.Error("no peers for block")
	return nil, ErrNoPeersBlock
}

func getWriteFromContext(ctx context.Context) agro.WriteLevel {
	v, ok := ctx.Value(ctxWriteLevel).(agro.WriteLevel)
	if ok {
		return v
	}
	return agro.WriteAll
}

func getReadFromContext(ctx context.Context) agro.ReadLevel {
	v, ok := ctx.Value(ctxReadLevel).(agro.ReadLevel)
	if ok {
		return v
	}
	return agro.ReadBlock
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
	switch getWriteFromContext(ctx) {
	case agro.WriteLocal:
		err = d.blocks.WriteBlock(ctx, i, data)
		return err
	case agro.WriteOne:
		for _, p := range peers.Peers[:peers.Replication] {
			// If we're one of the desired peers, we count, write here first.
			if p == d.srv.mds.UUID() {
				err = d.blocks.WriteBlock(ctx, i, data)
				if err != nil {
					clog.Errorf("WriteOne error, local: %s", err)
				} else {
					return nil
				}
			}
		}
		for _, p := range peers.Peers[:peers.Replication] {
			if p == d.srv.mds.UUID() {
				continue
			}
			err = d.client.PutBlock(ctx, p, i, data)
			if err == nil {
				return nil
			}
			clog.Errorf("WriteOne error, remote: %s", err)
		}
		return agro.ErrNoPeer
	case agro.WriteAll:
		for _, p := range peers.Peers[:peers.Replication] {
			var err error
			if p == d.srv.mds.UUID() {
				err = d.blocks.WriteBlock(ctx, i, data)
			} else {
				err = d.client.PutBlock(ctx, p, i, data)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *distributor) HasBlock(ctx context.Context, i agro.BlockRef) (bool, error) {
	return false, errors.New("unimplemented -- finding if a block exists cluster-wide")
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
