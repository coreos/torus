package server

import (
	"github.com/barakmich/agro"
	"github.com/barakmich/agro/ring"
)

// Goroutine which watches for new rings and kicks off
// the rebalance dance.
func (d *distributor) rebalancer(closer chan struct{}) {
	ch := make(chan agro.Ring)
	d.srv.mds.SubscribeNewRings(ch)
	for {
		select {
		case <-closer:
			d.srv.mds.UnsubscribeNewRings(ch)
			close(ch)
			break
		case newring, ok := <-ch:
			if ok {
				if newring.Version() == d.ring.Version() {
					// No problem. We're seeing the same ring.
					continue
				}
				if newring.Version() != d.ring.Version()+1 {
					panic("replacing old ring with ring in the far future!")
				}
				d.Rebalance(newring)
			} else {
				break
			}
		}
	}
}

func (d *distributor) Rebalance(newring agro.Ring) {
	// TODO(barakmich): Rebalancing is tricky. But here's the entry point.
	clog.Infof("rebalancing beginning: new ring version %d", newring.Version())
	d.mut.Lock()
	defer d.mut.Unlock()
	// TODO(barakmich): This is indeed a bad way to rebalance. The correct way is
	// an algorithm which is agnostic to the type of ring, but asks the correct
	// questions of the ring abstraction to run through a rebalance cycle.
	//
	// However, for prototype purposes, we can do the following:
	//
	if d.ring.Type() == ring.Empty {
		// We can always replace the empty ring.
		clog.Infof("replacing empty ring")
		d.ring = newring
		return
	}
	// Whee. Only one ring, that we know works. Beyond that:
	panic("unimplemented: couldn't rebalance")
}
