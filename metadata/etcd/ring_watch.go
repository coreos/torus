package etcd

import (
	"golang.org/x/net/context"

	"github.com/coreos/torus"
	"github.com/coreos/torus/ring"
)

//watch the ring updation event.

func (e *Etcd) watchRingUpdates() error {
	r, err := e.GetRing()
	if err != nil {
		clog.Errorf("can't get inital ring: %s", err)
		return err
	}
	go e.watchRing(r)
	return nil
}

func (e *Etcd) watchRing(r torus.Ring) {
	ctx, cancel := context.WithCancel(e.getContext())
	defer cancel()
	wch := e.Client.Watch(ctx, MkKey("meta", "the-one-ring"))

	for resp := range wch {
		if err := resp.Err(); err != nil {
			clog.Errorf("error watching ring: %s", err)
			return
		}
		for _, ev := range resp.Events {
			newRing, err := ring.Unmarshal(ev.Kv.Value)
			if err != nil {
				clog.Debugf("corrupted ring: %#v", ev.Kv.Value)
				clog.Errorf("Failed to unmarshal ring: %s", err)
				clog.Error("corrupted ring? Continuing with current ring")
				continue
			}

			clog.Infof("got new ring")
			if r.Version() == newRing.Version() {
				clog.Warningf("Same ring version: %d", r.Version())
			}
			e.mut.RLock()
			for _, x := range e.ringListeners {
				x <- newRing
			}
			r = newRing
			e.mut.RUnlock()
		}
	}
}
