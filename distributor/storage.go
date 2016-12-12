package distributor

import (
	"errors"
	"sync"
	"time"

	"github.com/coreos/torus"
	"golang.org/x/net/context"
)

var (
	ErrNoPeersBlock = errors.New("distributor: no peers available for a block")
)

func (d *Distributor) GetBlock(ctx context.Context, i torus.BlockRef) ([]byte, error) {
	d.mut.RLock()
	defer d.mut.RUnlock()
	promDistBlockRequests.Inc()
	bcache, ok := d.readCache.Get(string(i.ToBytes()))
	if ok {
		promDistBlockCacheHits.Inc()
		return bcache.([]byte), nil
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
	writeLevel := d.getWriteFromServer()
	for _, p := range peers.Peers[:peers.Replication] {
		if p == d.UUID() || writeLevel == torus.WriteLocal {
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
	readLevel := d.getReadFromServer()
	switch readLevel {
	case torus.ReadBlock:
		blk, err = d.readWithBackoff(ctx, i, peers)
	case torus.ReadSequential:
		blk, err = d.readSequential(ctx, i, peers, clientTimeout)
	case torus.ReadSpread:
		blk, err = d.readSpread(ctx, i, peers)
	default:
		panic("unhandled read level")
	}
	if err != nil {
		// We completely failed!
		promDistBlockFailures.Inc()
		clog.Errorf("no peers for block %s: %v", i, err)
	}
	return blk, err
}

func (d *Distributor) readWithBackoff(ctx context.Context, ref torus.BlockRef, peers torus.PeerPermutation) ([]byte, error) {
	for i := uint(0); i < 10; i++ {
		timeout := clientTimeout * (1 << i)
		blk, err := d.readSequential(ctx, ref, peers, timeout)
		if err == nil {
			return blk, err
		}
		clog.Warningf("failed peers, retry count %d: %v", i, err)
	}
	return nil, ErrNoPeersBlock
}

func (d *Distributor) readSequential(ctx context.Context, i torus.BlockRef, peers torus.PeerPermutation, timeout time.Duration) ([]byte, error) {
	for _, p := range peers.Peers {
		// If it's local, just try to get it.
		if p == d.UUID() {
			b, err := d.blocks.GetBlock(ctx, i)
			if err == nil {
				promDistBlockLocalHits.Inc()
				return b, nil
			}
			promDistBlockLocalFailures.Inc()
			clog.Debugf("failed local peer (again): %s", err)
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
		if err == torus.ErrBlockUnavailable || err == torus.ErrNoPeer {
			clog.Warningf("block %s from %s failed, trying next peer", i, p)
			promDistBlockPeerFailures.WithLabelValues(p).Inc()
			continue
		}

		// If there was a more significant error, fail hard.
		promDistBlockFailures.Inc()
		clog.Errorf("failed remote peer %s: %s", p, err)
		return nil, err
	}
	return nil, ErrNoPeersBlock
}

func (d *Distributor) readSpread(ctx context.Context, i torus.BlockRef, peers torus.PeerPermutation) ([]byte, error) {
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
			clog.Debugf("failed spread-read %s: %s", i, err)
			count--
			if count == 0 {
				return nil, ErrNoPeersBlock
			}
		}
	}
}

func (d *Distributor) readFromPeer(ctx context.Context, i torus.BlockRef, peer string) ([]byte, error) {
	blk, err := d.client.GetBlock(ctx, peer, i)
	// If we're successful, store that.
	if err == nil {
		d.readCache.Put(string(i.ToBytes()), blk)
		promDistBlockPeerHits.WithLabelValues(peer).Inc()
		return blk, nil
	}
	return nil, err
}

func (d *Distributor) getWriteFromServer() torus.WriteLevel {
	return d.srv.Cfg.WriteLevel
}

func (d *Distributor) getReadFromServer() torus.ReadLevel {
	return d.srv.Cfg.ReadLevel
}

func (d *Distributor) WriteBlock(ctx context.Context, i torus.BlockRef, data []byte) error {
	d.mut.RLock()
	defer d.mut.RUnlock()
	peers, err := d.ring.GetPeers(i)
	if err != nil {
		return err
	}
	if len(peers.Peers) == 0 {
		return ErrNoPeersBlock
	}
	d.readCache.Put(string(i.ToBytes()), data)
	switch d.getWriteFromServer() {
	case torus.WriteLocal:
		err = d.blocks.WriteBlock(ctx, i, data)
		if err == nil {
			return nil
		}
		clog.Debugf("Couldn't write locally; writing to cluster: %s", err)
		fallthrough
	case torus.WriteOne:
		for _, p := range peers.Peers[:peers.Replication] {
			// If we're one of the desired peers, we count, write here first.
			if p == d.UUID() {
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
		return torus.ErrNoPeer
	case torus.WriteAll:
		toWrite := peers.Replication
		for _, p := range peers.Peers {
			var err error
			if p == d.UUID() {
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
			clog.Noticef("error WriteAll to all peers")
			return torus.ErrNoPeer
		}
		clog.Warningf("only wrote block to %d/%d peers", (peers.Replication - toWrite), peers.Replication)
	}
	return nil
}

func (d *Distributor) WriteBuf(ctx context.Context, i torus.BlockRef) ([]byte, error) {
	return d.blocks.WriteBuf(ctx, i)
}

func (d *Distributor) HasBlock(ctx context.Context, i torus.BlockRef) (bool, error) {
	return false, errors.New("unimplemented -- finding if a block exists cluster-wide")
}

func (d *Distributor) DeleteBlock(ctx context.Context, i torus.BlockRef) error {
	return d.blocks.DeleteBlock(ctx, i)
}

func (d *Distributor) NumBlocks() uint64 {
	d.mut.RLock()
	defer d.mut.RUnlock()
	return d.blocks.NumBlocks()
}

func (d *Distributor) UsedBlocks() uint64 {
	d.mut.RLock()
	defer d.mut.RUnlock()
	return d.blocks.UsedBlocks()
}

func (d *Distributor) BlockIterator() torus.BlockIterator {
	return d.blocks.BlockIterator()
}

func (d *Distributor) Flush() error {
	return d.blocks.Flush()
}

func (d *Distributor) Kind() string { return "distributor" }

func (d *Distributor) BlockSize() uint64 {
	return d.blocks.BlockSize()
}
