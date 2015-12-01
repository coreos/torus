package server

import (
	"github.com/barakmich/agro"
	"github.com/barakmich/agro/ring"
)

// Goroutine which watches for new rings and kicks off
// the rebalance dance.
func (d *distributor) rebalancer() {
	ch := make(chan agro.Ring)
	d.srv.mds.SubscribeNewRings(ch)
	for newring := range ch {
		if newring.Version() == d.ring.Version() {
			// No problem. We're seeing the same ring.
			continue
		}
		if newring.Version() != d.ring.Version()+1 {
			panic("replacing old ring with ring in the far future!")
		}
		d.Rebalance(newring)
	}
}

func (d *distributor) Rebalance(newring agro.Ring) {
	// TODO(barakmich): Rebalancing is tricky. But here's the entry point.
	clog.Infof("rebalancing beginning: new ring version %d", newring.Version())
	d.mut.Lock()
	defer d.mut.Unlock()
	if d.ring.Type() == ring.Empty {
		// We can always replace the empty ring.
		clog.Infof("replacing empty ring")
		d.ring = newring
		return
	}
	panic("couldn't rebalance")
}
