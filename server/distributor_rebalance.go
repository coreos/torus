package server

import (
	"time"

	"github.com/coreos/agro"
)

var (
	rebalanceTimeout = 30 * time.Second
)

// Goroutine which watches for new rings and kicks off
// the rebalance dance.
func (d *distributor) rebalanceWatcher(closer chan struct{}) {
	ch := make(chan agro.Ring)
	d.srv.mds.SubscribeNewRings(ch)
exit:
	for {
		select {
		case <-closer:
			d.srv.mds.UnsubscribeNewRings(ch)
			close(ch)
			break exit
		case newring, ok := <-ch:
			if ok {
				if newring.Version() == d.ring.Version() {
					// No problem. We're seeing the same ring.
					continue
				}
				if newring.Version() != d.ring.Version()+1 {
					panic("replacing old ring with ring in the far future!")
				}
				d.mut.Lock()
				d.ring = newring
				d.mut.Unlock()
			} else {
				break exit
			}
		}
	}
}
