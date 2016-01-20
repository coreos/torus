package etcd

import (
	"errors"
	"fmt"
	"io"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"

	etcdpb "github.com/coreos/agro/internal/etcdproto/etcdserverpb"
)

func (c *etcdCtx) OpenRebalanceChannels() (inOut [2]chan *models.RebalanceStatus, leader bool, err error) {
	// Leader elect
	// TODO(barakmich): LEASE
	tx := tx().If(
		keyNotExists(mkKey("meta", "rebalance-leader")),
	).Then(
		setKey(mkKey("meta", "rebalance-leader"), []byte(c.UUID())),
	).Else(
		getKey(mkKey("meta", "rebalance-leader")),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if err != nil {
		return [2]chan *models.RebalanceStatus{nil, nil}, false, err
	}
	if resp.Succeeded {
		revision := resp.Responses[0].GetResponsePut().Header.Revision
		return c.openRebalanceLeader(revision)
	}
	revision := resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
	return c.openRebalanceFollower(revision)
}

func (c *etcdCtx) SetRebalanceSnapshot(kind uint64, data []byte) error {
	if data == nil {
		tx := tx().Do(
			deleteKey(mkKey("meta", "rebalance-snapshot")),
			deleteKey(mkKey("meta", "rebalance-kind")),
		).Tx()
		_, err := c.etcd.kv.Txn(c.getContext(), tx)
		return err
	}
	tx := tx().Do(
		setKey(mkKey("meta", "rebalance-kind"), uint64ToBytes(kind)),
		setKey(mkKey("meta", "rebalance-snapshot"), data),
	).Tx()
	_, err := c.etcd.kv.Txn(c.getContext(), tx)
	return err
}

func (c *etcdCtx) GetRebalanceSnapshot() (uint64, []byte, error) {
	tx := tx().Do(
		getKey(mkKey("meta", "rebalance-kind")),
		getKey(mkKey("meta", "rebalance-snapshot")),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if err != nil {
		return 0, nil, err
	}
	resps := resp.GetResponses()
	if len(resps) != 2 {
		return 0, nil, agro.ErrNotExist
	}
	return bytesToUint64(resps[0].GetResponseRange().Kvs[0].Value),
		resps[1].GetResponseRange().Kvs[0].Value, err
}

func (c *etcdCtx) openRebalanceLeader(rev int64) (inOut [2]chan *models.RebalanceStatus, leader bool, err error) {
	toC := make(chan *models.RebalanceStatus)
	fromC := make(chan *models.RebalanceStatus)
	err = c.leaderWatch(toC, fromC, rev)
	if err != nil {
		tx := tx().Do(
			deleteKey(mkKey("meta", "rebalance-leader")),
			deleteKey(mkKey("meta", "rebalance-leader-status")),
			deleteKey(mkKey("meta", "rebalance-status", c.UUID())),
		).Tx()
		_, derr := c.etcd.kv.Txn(c.getContext(), tx)
		if derr != nil {
			clog.Error(derr)
			err = errors.New(fmt.Sprintf("combined err: %s %s", err.Error(), derr.Error()))
		}
		return [2]chan *models.RebalanceStatus{nil, nil}, true, err
	}
	return [2]chan *models.RebalanceStatus{toC, fromC}, true, nil
}

func (c *etcdCtx) leaderWatch(toC chan *models.RebalanceStatus, fromC chan *models.RebalanceStatus, rev int64) error {
	wAPI := etcdpb.NewWatchClient(c.etcd.conn)
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
	p := &etcdpb.WatchRequest{
		RequestUnion: &etcdpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdpb.WatchCreateRequest{
				Prefix:        mkKey("meta", "rebalance-status"),
				StartRevision: rev,
			},
		},
	}
	err = wStream.Send(p)
	return err
}

func (c *etcdCtx) openRebalanceFollower(rev int64) (inOut [2]chan *models.RebalanceStatus, leader bool, err error) {
	toC := make(chan *models.RebalanceStatus)
	fromC := make(chan *models.RebalanceStatus)
	err = c.followWatch(toC, fromC, rev)
	if err != nil {
		_, derr := c.etcd.kv.DeleteRange(c.getContext(),
			deleteKey(mkKey("meta", "rebalance-status", c.UUID())))
		if derr != nil {
			err = errors.New(fmt.Sprintf("combined err: %s %s", err.Error(), derr.Error()))
		}
		return [2]chan *models.RebalanceStatus{nil, nil}, false, err
	}
	return [2]chan *models.RebalanceStatus{toC, fromC}, false, nil
}

func (c *etcdCtx) followWatch(toC chan *models.RebalanceStatus, fromC chan *models.RebalanceStatus, rev int64) error {
	wAPI := etcdpb.NewWatchClient(c.etcd.conn)
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
					err = errors.New(fmt.Sprintf("combined err: %s %s", err.Error(), derr.Error()))
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
	p := &etcdpb.WatchRequest{
		RequestUnion: &etcdpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdpb.WatchCreateRequest{
				Key:           mkKey("meta", "rebalance-leader-status"),
				StartRevision: rev,
			},
		},
	}
	err = wStream.Send(p)
	return err
}

func watchStream(wStream etcdpb.Watch_WatchClient, c chan *models.RebalanceStatus) {
	for {
		resp, err := wStream.Recv()
		if err == io.EOF {
			close(c)
			return
		}
		if err != nil {
			// TODO(barakmich): Pass an error in RebalanceStatus
			clog.Fatalf("error watching stream: %s SHOULD BE ABLE TO CONTINUE (SEE TODO)", err)
			break
		}
		switch {
		case resp.Canceled, resp.Compacted:
			continue
		}
		for _, ev := range resp.Events {
			m := &models.RebalanceStatus{}
			err := m.Unmarshal(ev.Kv.Value)
			if err != nil {
				clog.Debugf("corrupted data: %#v", ev.Kv.Value)
				clog.Error("corrupted rebalance data?")
				continue
			}
			c <- m
		}
	}

}
