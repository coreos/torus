package server

import (
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"
)

type RebalanceStrategy int32

const (
	Error RebalanceStrategy = iota
	Replace
)

var rebalanceTimeout = 30 * time.Second

// Goroutine which watches for new rings and kicks off
// the rebalance dance.
func (d *distributor) rebalancer(closer chan struct{}) {
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
				d.Rebalance(newring)
			} else {
				break exit
			}
		}
	}
}

func (d *distributor) Rebalance(newring agro.Ring) {
	// TODO(barakmich): Rebalancing is tricky. But here's the entry point.
	clog.Infof("rebalancing beginning: new ring version %d", newring.Version())
	// TODO(barakmich): This is indeed a bad way to rebalance. The correct way is
	// an algorithm which is agnostic to the type of ring, but asks the correct
	// questions of the ring abstraction to run through a rebalance cycle.
	//
	// However, for prototype purposes, we can do the following:
	//
	chans, leader, err := d.srv.mds.OpenRebalanceChannels()
	if err != nil {
		clog.Error(err)
		return
	}
	if leader {
		clog.Infof("elected as leader")
		d.rebalanceLeader(chans, newring)
		return
	}
	d.rebalanceFollower(chans, newring)
	if d.ring.Type() == ring.Empty {
		// We can always replace the empty ring.
		clog.Infof("replacing empty ring")
		d.ring = newring
		return
	}
	// Whee. Only one ring, that we know works. Beyond that:
	panic("unimplemented: couldn't rebalance")
}

func (d *distributor) rebalanceLeader(inOut [2]chan *models.RebalanceStatus, newring agro.Ring) {

}

func (d *distributor) rebalanceFollower(inOut [2]chan *models.RebalanceStatus, newring agro.Ring) {
	in, out := inOut[0], inOut[1]
	for {
		select {
		case s := <-in:

		case <-time.After(rebalanceTimeout):
			close(out)
			// Re-elect
			d.Rebalance(newring)
		}
	}
}
