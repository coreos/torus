package rebalance

import (
	"io"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
)

const maxIters = 20

var rebalanceTimeout = 5 * time.Second

func (r *rebalancer) Tick() (int, error) {
	if r.it == nil {
		r.version = r.r.Ring().Version()
		r.it = r.bs.BlockIterator()
	}
	m := make(map[string][]agro.BlockRef)
	toDelete := make(map[agro.BlockRef]bool)
	ring := r.r.Ring()

outer:
	for i := 0; i < maxIters; i++ {
		var ref agro.BlockRef
		for {
			ok := r.it.Next()
			if !ok {
				err := r.it.Err()
				if err != nil {
					return 0, err
				}
				r.it = nil
				break outer
			}
			ref = r.it.BlockRef()
			if r.vol != 0 && ref.Volume() == r.vol {
				break
			}
		}
		if r.gc.IsDead(ref) {
			toDelete[ref] = true
			i--
			continue
		}
		perm, err := ring.GetPeers(ref)
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
					clog.Debug("couldn't get local block")
					// The GC came around underneath us. It's okay
					// to keep working as normal.
					continue
				}
				n++
				ctx, cancel := context.WithTimeout(context.TODO(), rebalanceTimeout)
				err = r.cs.PutBlock(ctx, k, v[i], data)
				cancel()
				if err != nil {
					// Continue for now
					toDelete[v[i]] = false
					clog.Error(err)
				}
			}
		}
	}

	for k, v := range toDelete {
		if v {
			err := r.bs.DeleteBlock(context.TODO(), k)
			if err != nil {
				clog.Error("couldn't delete local block")
				return n, err
			}
		}
	}

	var outerr error
	if r.it == nil {
		outerr = io.EOF
	}
	return n, outerr
}
