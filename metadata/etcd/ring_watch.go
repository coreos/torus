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

	p := &pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key: mkKey("meta", "the-one-ring"),
			},
		},
	}
	err = wStream.Send(p)
	return err
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
		switch {
		case resp.Created:
			continue
		case resp.Canceled:
			continue
		case resp.Compacted:
			continue
		}
		for _, ev := range resp.Events {
			newRing, err := ring.Unmarshal(ev.Kv.Value)
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

}
