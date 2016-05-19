package rebalance

import (
	"io"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
	"github.com/coreos/pkg/capnslog"
)

const maxIters = 50

var rebalanceTimeout = 5 * time.Second

func (r *rebalancer) Tick() (int, error) {
	if r.it == nil {
		r.it = r.bs.BlockIterator()
		r.ring = r.r.Ring()
	}
	m := make(map[string][]agro.BlockRef)
	toDelete := make(map[agro.BlockRef]bool)
	dead := make(map[agro.BlockRef]bool)
	itDone := false

	for i := 0; i < maxIters; i++ {
		var ref agro.BlockRef
		ok := r.it.Next()
		if !ok {
			err := r.it.Err()
			if err != nil {
				return 0, err
			}
			itDone = true
			break
		}
		ref = r.it.BlockRef()
		if r.gc.IsDead(ref) {
			dead[ref] = true
			continue
		}
		perm, err := r.ring.GetPeers(ref)
		if err != nil {
			return 0, err
		}
		desired := agro.PeerList(perm.Peers[:perm.Replication])
		myIndex := desired.IndexAt(r.r.UUID())
		for j, p := range desired {
			if j == myIndex {
				continue
			}
			m[p] = append(m[p], ref)
		}
		if myIndex == -1 {
			toDelete[ref] = true
		}
	}

	n := 0
	for k, v := range m {
		ctx, cancel := context.WithTimeout(context.TODO(), rebalanceTimeout)
		oks, err := r.cs.Check(ctx, k, v)
		cancel()
		if err != nil {
			for _, blk := range v {
				toDelete[blk] = false
			}
			if err != agro.ErrNoPeer {
				clog.Error(err)
			}
			continue
		}
		for i, ok := range oks {
			if !ok {
				data, err := r.bs.GetBlock(context.TODO(), v[i])
				if err != nil {
					clog.Warning("couldn't get local block -- why would this happen?")
					continue
				}
				n++
				ctx, cancel := context.WithTimeout(context.TODO(), rebalanceTimeout)
				if agro.BlockLog.LevelAt(capnslog.TRACE) {
					agro.BlockLog.Tracef("rebalance: sending block %s to %s", v[i], k)
				}
				err = r.cs.PutBlock(ctx, k, v[i], data)
				cancel()
				if err != nil {
					// Continue for now
					toDelete[v[i]] = false
					clog.Errorf("couldn't rebalance block %s: %v", v[i], err)
				}
			}
		}
	}

	for k, v := range toDelete {
		if v {
			if agro.BlockLog.LevelAt(capnslog.TRACE) {
				agro.BlockLog.Tracef("rebalance: deleting replicated block %s", k)
			}
			err := r.bs.DeleteBlock(context.TODO(), k)
			if err != nil {
				clog.Error("couldn't delete local block")
				return n, err
			}
		}
	}

	for k, v := range dead {
		if v {
			if agro.BlockLog.LevelAt(capnslog.TRACE) {
				agro.BlockLog.Tracef("rebalance: deleting dead block %s", k)
			}
			err := r.bs.DeleteBlock(context.TODO(), k)
			if err != nil {
				clog.Error("couldn't delete local block")
				return n, err
			}
		}
	}
	r.bs.Flush()

	var outerr error
	if itDone {
		outerr = io.EOF
	}
	return n, outerr
}
