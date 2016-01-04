package etcd

import (
	"io"

	"golang.org/x/net/context"

	pb "github.com/coreos/agro/internal/etcdproto/etcdserverpb"
	"github.com/coreos/agro/ring"
)

func (e *etcd) watchRingUpdates() error {
	wAPI := pb.NewWatchClient(e.conn)
	wStream, err := wAPI.Watch(context.TODO())
	if err != nil {
		return err
	}
	go e.watchRing(wStream)

	err = wStream.Send(&pb.WatchRequest{
		Key: mkKey("meta", "the-one-ring"),
	})
	if err != nil {
		return err
	}

	return nil
}

func (e *etcd) watchRing(wStream pb.Watch_WatchClient) {
	r, err := e.GetRing()
	if err != nil {
		clog.Errorf("can't get inital ring: %s", err)
		return
	}
	for {
		resp, err := wStream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			clog.Errorf("error watching ring: %s", err)
			break
		}
		newRing, err := ring.Unmarshal(resp.Event.Kv.Value)
		if err != nil {
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
