package etcd

import (
	"encoding/json"

	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
	"github.com/coreos/torus/ring"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
)

func initEtcdMetadata(cfg torus.Config, gmd torus.GlobalMetadata, ringType torus.RingType, repFactor int) error {
	gmdbytes, err := json.Marshal(gmd)
	if err != nil {
		return err
	}
	emptyRing, err := ring.CreateRing(&models.Ring{
		Type:              uint32(ringType),
		Version:           1,
		ReplicationFactor: uint32(repFactor),
	})
	if err != nil {
		return err
	}
	ringb, err := emptyRing.Marshal()
	if err != nil {
		return err
	}

	client, err := etcdv3.New(etcdv3.Config{Endpoints: []string{cfg.MetadataAddress}, TLS: cfg.TLS})
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
		return torus.ErrExists
	}
	_, err = client.Put(context.Background(), MkKey("meta", "the-one-ring"), string(ringb))
	if err != nil {
		return err
	}
	return nil
}

func wipeEtcdMetadata(cfg torus.Config) error {
	client, err := etcdv3.New(etcdv3.Config{Endpoints: []string{cfg.MetadataAddress}, TLS: cfg.TLS})
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

func setRing(cfg torus.Config, r torus.Ring) error {
	client, err := etcdv3.New(etcdv3.Config{Endpoints: []string{cfg.MetadataAddress}, TLS: cfg.TLS})
	if err != nil {
		return err
	}
	defer client.Close()

	resp, err := client.Get(context.Background(), MkKey("meta", "the-one-ring"))
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return torus.ErrNoGlobalMetadata
	}
	oldr, err := ring.Unmarshal(resp.Kvs[0].Value)
	if err != nil {
		return err
	}
	if oldr.Version() != r.Version()-1 {
		return torus.ErrNonSequentialRing
	}
	b, err := r.Marshal()
	if err != nil {
		return err
	}
	_, err = client.Put(context.Background(), MkKey("meta", "the-one-ring"), string(b))
	return err
}
