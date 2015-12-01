package etcd

import (
	"time"

	"golang.org/x/net/context"

	pb "github.com/barakmich/agro/internal/etcdproto/etcdserverpb"
)

//TODO(barakmich): Use etcdv3 watches here, but they're broken right now. So instead, we'll poll.
func (e *etcd) watchRingUpdates() error {
	wc := pb.NewKVClient(e.conn)
	go e.watchRing(wc)
	return nil
}

func (e *etcd) watchRing(client pb.KVClient) {
	ring, err := e.GetRing()
	if err != nil {
		clog.Errorf("can't get inital ring: %s", err)
		return
	}
	for {
		resp, err := client.Range(context.TODO(), getKey(mkKey("meta", "the-one-ring")))
		if err != nil {
			clog.Errorf("error watching ring: %s", err)
			break
		}
		if len(resp.Kvs) == 0 {
			time.Sleep(5 * time.Second)
			continue
		}
		newRing, err := e.GetRing()
		if newRing.Version() == ring.Version() {
			time.Sleep(10 * time.Second)
			continue
		}
		clog.Infof("got new ring")
		e.mut.RLock()
		for _, x := range e.ringListeners {
			x <- newRing
		}
		ring = newRing
		e.mut.RUnlock()
		time.Sleep(10 * time.Second)
	}

}
