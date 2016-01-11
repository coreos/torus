package etcd

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
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
}

func (c *etcdCtx) openRebalanceFollower() (inOut [2]chan *models.RebalanceStatus, master bool, err error) {
}
