package etcd

import (
	"io"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"

	pb "github.com/coreos/agro/internal/etcdproto/etcdserverpb"
)

func (c *etcdCtx) OpenRebalanceChannels() (inOut [2]chan *models.RebalanceStatus, master bool, err error) {
	// Master elect
	// TODO(barakmich): LEASE
	tx := tx().If(
		keyNotExists(mkKey("meta", "rebalance-master")),
	).Then(
		setKey(mkKey("meta", "rebalance-master"), []byte(c.UUID())),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if err != nil {
		return [2]chan *models.RebalanceStatus{nil, nil}, false, err
	}
	if resp.Succeeded {
		return c.openRebalanceMaster()
	}
	return c.openRebalanceFollower()
}

func (c *etcdCtx) SetRebalanceSnapshot(x *models.RebalanceSnapshot) error {
	b, err := x.Marshal()
	if err != nil {
		return err
	}
	_, err = c.etcd.kv.Put(c.getContext(),
		setKey(mkKey("meta", "rebalance-snapshot"), b))
	if err != nil {
		return err
	}
	return nil
}

func (c *etcdCtx) GetRebalanceSnapshot() (*models.RebalanceSnapshot, error) {
	resp, err := c.etcd.kv.Range(c.getContext(),
		getKey(mkKey("meta", "rebalance-snapshot")))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, agro.ErrNotExist
	}
	d := &models.RebalanceSnapshot{}
	err = d.Unmarshal(resp.Kvs[0].Value)
	return d, err
}

func (c *etcdCtx) openRebalanceMaster() (inOut [2]chan *models.RebalanceStatus, master bool, err error) {
	toC := make(chan *models.RebalanceStatus)
	fromC := make(chan *models.RebalanceStatus)
	err = c.masterWatch(toC, fromC)
	if err != nil {
		_, derr := c.etcd.kv.DeleteRange(c.getContext(),
			deleteKey(mkKey("meta", "rebalance-snapshot")))
		if derr != nil {
			err = derr
		}
		return [2]chan *models.RebalanceStatus{nil, nil}, true, err
	}
	return [2]chan *models.RebalanceStatus{toC, fromC}, true, nil
}

func (c *etcdCtx) masterWatch(toC chan *models.RebalanceStatus, fromC chan *models.RebalanceStatus) error {
	wAPI := pb.NewWatchClient(c.etcd.conn)
	wStream, err := wAPI.Watch(c.getContext())
	if err != nil {
		return err
	}
	go watchStream(wStream, toC)
	go func() {
		for {
			stat, ok := <-fromC
			if !ok {
				wStream.CloseSend()
				return
			}
			b, err := stat.Marshal()
			if err != nil {
				panic(err)
			}
			tx := tx().Do(
				setKey(mkKey("meta", "rebalance-status", c.UUID()), b),
				setKey(mkKey("meta", "rebalance-master-status"), b),
			).Tx()
			_, err = c.etcd.kv.Txn(c.getContext(), tx)
			if err != nil {
				clog.Fatal(err)
			}
		}
	}()
	p := &pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Prefix:        mkKey("meta", "rebalance-status"),
				StartRevision: 1,
			},
		},
	}
	err = wStream.Send(p)
	return err
}

func (c *etcdCtx) openRebalanceFollower() (inOut [2]chan *models.RebalanceStatus, master bool, err error) {
	toC := make(chan *models.RebalanceStatus)
	fromC := make(chan *models.RebalanceStatus)
	err = c.followWatch(toC, fromC)
	if err != nil {
		_, derr := c.etcd.kv.DeleteRange(c.getContext(),
			deleteKey(mkKey("meta", "rebalance-snapshot")))
		if derr != nil {
			err = derr
		}
		return [2]chan *models.RebalanceStatus{nil, nil}, false, err
	}
	return [2]chan *models.RebalanceStatus{toC, fromC}, false, nil
}

func (c *etcdCtx) followWatch(toC chan *models.RebalanceStatus, fromC chan *models.RebalanceStatus) error {
	wAPI := pb.NewWatchClient(c.etcd.conn)
	wStream, err := wAPI.Watch(c.getContext())
	if err != nil {
		return err
	}
	go watchStream(wStream, toC)
	go func() {
		for {
			stat, ok := <-fromC
			if !ok {
				wStream.CloseSend()
				return
			}
			b, err := stat.Marshal()
			if err != nil {
				panic(err)
			}
			tx := tx().Do(
				setKey(mkKey("meta", "rebalance-status", c.UUID()), b),
			).Tx()
			_, err = c.etcd.kv.Txn(c.getContext(), tx)
			if err != nil {
				clog.Fatal(err)
			}
		}
	}()
	p := &pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key:           mkKey("meta", "rebalance-master-status"),
				StartRevision: 1,
			},
		},
	}
	err = wStream.Send(p)
	return err
}

func watchStream(wStream pb.Watch_WatchClient, c chan *models.RebalanceStatus) {
	for {
		resp, err := wStream.Recv()
		if err == io.EOF {
			close(c)
			return
		}
		if err != nil {
			clog.Errorf("error watching stream: %s", err)
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
			clog.Tracef("got new data at %s", ev.Kv.Key)
			m := &models.RebalanceStatus{}
			err := m.Unmarshal(ev.Kv.Value)
			if err != nil {
				clog.Error("corrupted rebalance data?")
				continue
			}
			c <- m
		}
	}

}
