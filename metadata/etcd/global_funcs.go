package etcd

import (
	"encoding/json"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
)

func initEtcdMetadata(cfg agro.Config, gmd agro.GlobalMetadata, ringType agro.RingType) error {
	gmdbytes, err := json.Marshal(gmd)
	if err != nil {
		return err
	}
	emptyRing, err := ring.CreateRing(&models.Ring{
		Type:              uint32(ringType),
		Version:           1,
		ReplicationFactor: 2,
	})
	if err != nil {
		return err
	}
	ringb, err := emptyRing.Marshal()
	if err != nil {
		return err
	}

	client, err := etcdv3.New(etcdv3.Config{Endpoints: []string{cfg.MetadataAddress}})
	if err != nil {
		return err
	}
	defer client.Close()

	txn := client.Txn(context.Background())
	resp, err := txn.If(
		etcdv3.Compare(etcdv3.Version(MkKey("meta", "globalmetadata")), "=", 0),
	).Then(
		etcdv3.OpPut(MkKey("meta", "volumeminter"), string(Uint64ToBytes(1))),
		etcdv3.OpPut(MkKey("meta", "globalmetadata"), string(gmdbytes)),
	).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrExists
	}
	_, err = client.Put(context.Background(), MkKey("meta", "the-one-ring"), string(ringb))
	if err != nil {
		return err
	}
	return nil
}

func wipeEtcdMetadata(cfg agro.Config) error {
	client, err := etcdv3.New(etcdv3.Config{Endpoints: []string{cfg.MetadataAddress}})
	if err != nil {
		return err
	}
	defer client.Close()
	_, err = client.Delete(context.Background(), MkKey(), etcdv3.WithPrefix())
	if err != nil {
		return err
	}
	return nil
}

func setRing(cfg agro.Config, r agro.Ring) error {
	client, err := etcdv3.New(etcdv3.Config{Endpoints: []string{cfg.MetadataAddress}})
	if err != nil {
		return err
	}
	defer client.Close()

	resp, err := client.Get(context.Background(), MkKey("meta", "the-one-ring"))
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return agro.ErrNoGlobalMetadata
	}
	oldr, err := ring.Unmarshal(resp.Kvs[0].Value)
	if err != nil {
		return err
	}
	if oldr.Version() != r.Version()-1 {
		return agro.ErrNonSequentialRing
	}
	b, err := r.Marshal()
	if err != nil {
		return err
	}
	_, err = client.Put(context.Background(), MkKey("meta", "the-one-ring"), string(b))
	return err
}
