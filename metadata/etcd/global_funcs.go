package etcd

import (
	"encoding/json"

	"github.com/coreos/agro"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	etcdpb "github.com/coreos/agro/internal/etcdproto/etcdserverpb"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"
)

func mkfs(cfg agro.Config, gmd agro.GlobalMetadata, ringType agro.RingType) error {
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
	tx := tx().If(
		keyNotExists(mkKey("meta", "globalmetadata")),
	).Then(
		setKey(mkKey("meta", "volumeminter"), uint64ToBytes(1)),
		setKey(mkKey("meta", "globalmetadata"), gmdbytes),
	).Tx()
	conn, err := grpc.Dial(cfg.MetadataAddress, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	client := etcdpb.NewKVClient(conn)
	resp, err := client.Txn(context.Background(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrExists
	}
	_, err = client.Put(context.Background(),
		setKey(mkKey("meta", "the-one-ring"), ringb))
	if err != nil {
		return err
	}
	return nil
}

func setRing(cfg agro.Config, r agro.Ring) error {
	b, err := r.Marshal()
	if err != nil {
		return err
	}
	conn, err := grpc.Dial(cfg.MetadataAddress, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	client := etcdpb.NewKVClient(conn)
	resp, err := client.Range(context.Background(), getKey(mkKey("meta", "the-one-ring")))
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
	_, err = client.Put(context.Background(), setKey(mkKey("meta", "the-one-ring"), b))
	return err
}
