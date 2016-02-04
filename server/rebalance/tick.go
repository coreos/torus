package rebalance

import (
	"io"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
)

const maxIters = 1

func (r *rebalancer) Tick() (int, error) {
	if r.it == nil {
		r.version = r.r.Ring().Version()
		r.it = r.bs.BlockIterator()
	}
	m := make(map[string][]agro.BlockRef)
	var toDelete []agro.BlockRef
	ring := r.r.Ring()
	for i := 0; i < maxIters; i++ {
		ok := r.it.Next()
		if !ok {
			err := r.it.Err()
			if err != nil {
				return 0, err
			}
			r.it = nil
			break
		}
		ref := r.it.BlockRef()
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
			toDelete = append(toDelete, ref)
		}
	}

	n := 0
	doDelete := true
	for k, v := range m {
		oks, err := r.cs.Check(context.TODO(), k, v)
		if err != nil {
			doDelete = false
			clog.Error(err)
			// Ignore for now; this is a never-ending loop
		}
		for i, ok := range oks {
			if !ok {
				data, err := r.bs.GetBlock(context.TODO(), v[i])
				if err != nil {
					return n, err
				}
				n++
				err = r.cs.PutBlock(context.TODO(), k, v[i], data)
				if err != nil {
					// Continue for now
					doDelete = false
					clog.Error(err)
				}
			}
		}
	}

	if doDelete {
		for _, x := range toDelete {
			err := r.bs.DeleteBlock(context.TODO(), x)
			if err != nil {
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
