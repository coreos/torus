package etcd

import (
	"io"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"

	pb "github.com/coreos/agro/internal/etcdproto/etcdserverpb"
)

func (c *etcdCtx) OpenRebalanceChannels() (inOut [2]chan *models.RebalanceStatus, leader bool, err error) {
	// Leader elect
	// TODO(barakmich): LEASE
	tx := tx().If(
		keyNotExists(mkKey("meta", "rebalance-leader")),
	).Then(
		setKey(mkKey("meta", "rebalance-leader"), []byte(c.UUID())),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if err != nil {
		return [2]chan *models.RebalanceStatus{nil, nil}, false, err
	}
	if resp.Succeeded {
		return c.openRebalanceLeader()
	}
	return c.openRebalanceFollower()
}

func (c *etcdCtx) SetRebalanceSnapshot(x *models.RebalanceSnapshot) error {
	if x == nil {
		_, err := c.etcd.kv.DeleteRange(c.getContext(),
			deleteKey(mkKey("meta", "rebalance-snapshot")))
		return err
	}
	b, err := x.Marshal()
	if err != nil {
		return err
	}
	_, err = c.etcd.kv.Put(c.getContext(),
		setKey(mkKey("meta", "rebalance-snapshot"), b))
	return err
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

func (c *etcdCtx) openRebalanceLeader() (inOut [2]chan *models.RebalanceStatus, leader bool, err error) {
	toC := make(chan *models.RebalanceStatus)
	fromC := make(chan *models.RebalanceStatus)
	err = c.leaderWatch(toC, fromC)
	if err != nil {
		tx := tx().Do(
			deleteKey(mkKey("meta", "rebalance-leader")),
			deleteKey(mkKey("meta", "rebalance-leader-status")),
			deleteKey(mkKey("meta", "rebalance-status", c.UUID())),
		).Tx()
		_, derr := c.etcd.kv.Txn(c.getContext(), tx)
		if derr != nil {
			err = derr
		}
		return [2]chan *models.RebalanceStatus{nil, nil}, true, err
	}
	return [2]chan *models.RebalanceStatus{toC, fromC}, true, nil
}

func (c *etcdCtx) leaderWatch(toC chan *models.RebalanceStatus, fromC chan *models.RebalanceStatus) error {
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
				tx := tx().Do(
					deleteKey(mkKey("meta", "rebalance-leader")),
					deleteKey(mkKey("meta", "rebalance-leader-status")),
					deleteKey(mkKey("meta", "rebalance-status", c.UUID())),
				).Tx()
				_, derr := c.etcd.kv.Txn(c.getContext(), tx)
				if derr != nil {
					clog.Error(derr)
				}
				return
			}
			b, err := stat.Marshal()
			if err != nil {
				panic(err)
			}
			tx := tx().Do(
				setKey(mkKey("meta", "rebalance-status", c.UUID()), b),
				setKey(mkKey("meta", "rebalance-leader-status"), b),
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
				Prefix: mkKey("meta", "rebalance-status"),
			},
		},
	}
	err = wStream.Send(p)
	return err
}

func (c *etcdCtx) openRebalanceFollower() (inOut [2]chan *models.RebalanceStatus, leader bool, err error) {
	toC := make(chan *models.RebalanceStatus)
	fromC := make(chan *models.RebalanceStatus)
	err = c.followWatch(toC, fromC)
	if err != nil {
		_, derr := c.etcd.kv.DeleteRange(c.getContext(),
			deleteKey(mkKey("meta", "rebalance-status", c.UUID())))
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
				_, derr := c.etcd.kv.DeleteRange(c.getContext(),
					deleteKey(mkKey("meta", "rebalance-status", c.UUID())))
				if derr != nil {
					err = derr
				}
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
				Key:           mkKey("meta", "rebalance-leader-status"),
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
		case resp.Canceled:
			continue
		case resp.Compacted:
			continue
		}
		for _, ev := range resp.Events {
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
