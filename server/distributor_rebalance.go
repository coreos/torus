package server

import (
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"
)

type RebalanceStrategy int32

type Rebalancer interface {
	Leader(d *distributor, chans [2]chan *models.RebalanceStatus, ring agro.Ring)
	AdvanceState(d *distributor, s *models.RebalanceStatus, ring agro.Ring) (*models.RebalanceStatus, bool, error)
	OnError(error) *models.RebalanceStatus
}

const (
	Error RebalanceStrategy = iota
	Replace
)

var (
	rebalanceTimeout   = 30 * time.Second
	rebalancerRegistry = make(map[RebalanceStrategy]func() Rebalancer)
)

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
	isMember := false
	for _, x := range d.ring.Members() {
		if x == d.UUID() {
			isMember = true
			break
		}
	}
	for _, x := range newring.Members() {
		if x == d.UUID() {
			isMember = true
			break
		}
	}
	if !isMember {
		clog.Infof("rebalance detected, but not a member")
		return
	}
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
}

func (d *distributor) rebalanceLeader(chans [2]chan *models.RebalanceStatus, newring agro.Ring) {
	var re Rebalancer
	switch d.ring.Type() {
	case ring.Empty:
		// We can always replace the empty ring.
		clog.Infof("replacing empty ring")
		re = rebalancerRegistry[Replace]()
	default:
		re = rebalancerRegistry[Error]()
	}
	re.Leader(d, chans, newring)
	d.srv.mut.Lock()
	defer d.srv.mut.Unlock()
	clog.Info("leader: success, setting new ring")
	d.ring = newring
	d.srv.mds.SetRing(newring, true)
	close(chans[1])
}

func (d *distributor) rebalanceFollower(inOut [2]chan *models.RebalanceStatus, newring agro.Ring) {
	in, out := inOut[0], inOut[1]
	var rebalancer Rebalancer
	for {
		select {
		case s := <-in:
			if !s.FromLeader {
				panic("got a message not from leader")
			}
			if rebalancer == nil {
				rebalancer = rebalancerRegistry[RebalanceStrategy(s.RebalanceType)]()
			}
			news, done, err := rebalancer.AdvanceState(d, s, newring)
			news.UUID = d.UUID()
			if err != nil {
				clog.Error(err)
				stat := rebalancer.OnError(err)
				if stat != nil {
					out <- stat
				}
				close(out)
				return
			}
			out <- news
			if done {
				close(out)
				d.srv.mut.Lock()
				defer d.srv.mut.Unlock()
				clog.Info("follower: success, setting new ring")
				d.ring = newring
				return
			}
		case <-time.After(rebalanceTimeout):
			close(out)
			// Re-elect
			d.Rebalance(newring)
		}
	}
}

func waitAll(c chan *models.RebalanceStatus, newring agro.Ring, phase int32) error {
	member := newring.Members()
	for len(member) > 0 {

		stat, ok := <-c
		if !ok {
			clog.Error("close before end of rebalance")
			return agro.ErrClosed
		}
		if stat.Phase == phase {
			for i, m := range member {
				if m == stat.UUID {
					clog.Debugf("got response from %s", stat.UUID)
					member = append(member[:i], member[i+1:]...)
					break
				}
			}
		}
	}
	clog.Debugf("finished waiting for members")
	return nil
}
