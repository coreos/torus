package etcd

import (
	"encoding/json"

	"github.com/barakmich/agro"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pb "github.com/barakmich/agro/internal/etcdproto/etcdserverpb"
	"github.com/barakmich/agro/models"
	"github.com/barakmich/agro/ring"
)

func mkfs(cfg agro.Config, gmd agro.GlobalMetadata) error {
	gmdbytes, err := json.Marshal(gmd)
	if err != nil {
		return err
	}
	emptyRing, err := ring.CreateRing(&models.Ring{
		Type:    uint32(ring.Empty),
		Version: 1,
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
		setKey(mkKey("meta", "inodeminter"), uint64ToBytes(1)),
		setKey(mkKey("meta", "globalmetadata"), gmdbytes),
		setKey(mkKey("meta", "the-one-ring"), ringb),
	).Tx()
	conn, err := grpc.Dial(cfg.MetadataAddress, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewKVClient(conn)
	resp, err := client.Txn(context.Background(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrExists
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
	client := pb.NewKVClient(conn)
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
