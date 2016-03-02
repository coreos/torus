package server

import (
	"errors"
	"sync"
	"time"

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
	var blk []byte
	readLevel := getReadFromContext(ctx)
	switch readLevel {
	case agro.ReadBlock:
		blk, err = d.readWithBackoff(ctx, i, peers)
	case agro.ReadSequential:
		blk, err = d.readSequential(ctx, i, peers, clientTimeout)
	case agro.ReadSpread:
		blk, err = d.readSpread(ctx, i, peers)
	default:
		panic("unhandled read level")
	}
	if err != nil {
		// We completely failed!
		promDistBlockFailures.Inc()
		clog.Error("no peers for block")
	}
	return blk, err
}

func (d *distributor) readWithBackoff(ctx context.Context, ref agro.BlockRef, peers agro.PeerPermutation) ([]byte, error) {
	for i := uint(0); i < 10; i++ {
		timeout := clientTimeout * (1 << i)
		blk, err := d.readSequential(ctx, ref, peers, timeout)
		if err == nil {
			return blk, err
		}
	}
	return nil, ErrNoPeersBlock
}

func (d *distributor) readSequential(ctx context.Context, i agro.BlockRef, peers agro.PeerPermutation, timeout time.Duration) ([]byte, error) {
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
		getctx, cancel := context.WithTimeout(ctx, timeout)
		blk, err := d.readFromPeer(getctx, i, p)
		cancel()

		if err == nil {
			return blk, nil
		}

		// If this peer didn't have it, continue
		if err == agro.ErrBlockUnavailable || err == agro.ErrNoPeer {
			clog.Warningf("block from %s failed, trying next peer", p)
			promDistBlockPeerFailures.WithLabelValues(p).Inc()
			continue
		}

		// If there was a more significant error, fail hard.
		promDistBlockFailures.Inc()
		clog.Errorf("failed remote peer %s %s %#v", p, err, err)
		return nil, err
	}
	return nil, ErrNoPeersBlock
}

func (d *distributor) readSpread(ctx context.Context, i agro.BlockRef, peers agro.PeerPermutation) ([]byte, error) {
	resch := make(chan []byte)
	errch := make(chan error, peers.Replication)
	var once sync.Once
	count := 0
	for _, p := range peers.Peers[:peers.Replication] {
		if p == d.UUID() {
			continue
		}
		go func(peer string) {
			getctx, cancel := context.WithTimeout(ctx, clientTimeout)
			blk, err := d.readFromPeer(getctx, i, peer)
			cancel()
			if err == nil {
				once.Do(func() {
					resch <- blk
					close(resch)
				})
				return
			}
			errch <- err
		}(p)
		count++
	}

	for {
		select {
		case blk := <-resch:
			return blk, nil
		case err := <-errch:
			clog.Debugf("spread-read: %s, %s", err, i)
			count--
			if count == 0 {
				return nil, ErrNoPeersBlock
			}
		}
	}
}

func (d *distributor) readFromPeer(ctx context.Context, i agro.BlockRef, peer string) ([]byte, error) {
	blk, err := d.client.GetBlock(ctx, peer, i)
	// If we're successful, store that.
	if err == nil {
		if d.readCache != nil {
			d.readCache.Put(string(i.ToBytes()), blk)
		}
		promDistBlockPeerHits.WithLabelValues(peer).Inc()
		return blk, nil
	}
	return nil, err
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
		if err == nil {
			return nil
		}
		clog.Noticef("Couldn't write locally; writing to cluster")
		// fallthrough is evil
		return d.WriteBlock(context.WithValue(ctx, ctxWriteLevel, agro.WriteOne), i, data)
	case agro.WriteOne:
		for _, p := range peers.Peers[:peers.Replication] {
			// If we're one of the desired peers, we count, write here first.
			if p == d.srv.mds.UUID() {
				err = d.blocks.WriteBlock(ctx, i, data)
				if err != nil {
					clog.Noticef("WriteOne error, local: %s", err)
				} else {
					return nil
				}
			}
		}
		for _, p := range peers.Peers {
			err = d.client.PutBlock(ctx, p, i, data)
			if err == nil {
				return nil
			}
			clog.Noticef("WriteOne error, remote: %s", err)
		}
		return agro.ErrNoPeer
	case agro.WriteAll:
		toWrite := peers.Replication
		for _, p := range peers.Peers {
			var err error
			if p == d.srv.mds.UUID() {
				err = d.blocks.WriteBlock(ctx, i, data)
			} else {
				err = d.client.PutBlock(ctx, p, i, data)
			}
			if err != nil {
				clog.Noticef("error WriteAll to peer %s: %s", p, err)
			} else {
				toWrite--
			}
			if toWrite == 0 {
				return nil
			}
		}
		if toWrite == peers.Replication {
			return err
		}
		clog.Warningf("only wrote block to %d/%d peers", toWrite, peers.Replication)
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
