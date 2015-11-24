package etcd

import (
	"path"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/blockset"
	"golang.org/x/net/context"

	// TODO(barakmich): And this is why vendoring sucks. I shouldn't need to
	//import this, but I do, because I'm using etcdserverpb from head, and *it*
	//expects its own vendored version. Admittedly, this should get better with
	//GO15VENDORING, but etcd doesn't support that yet.
	"github.com/coreos/etcd/Godeps/_workspace/src/google.golang.org/grpc"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

const keyPrefix = "/github.com/barakmich/agro/"

func init() {
	agro.RegisterMetadataService("etcd", newEtcdMetadata)
}

type etcd struct {
	cfg           agro.Config
	global        agro.GlobalMetadata
	volumeprinter uint64
	volumesCache  map[string]agro.VolumeID

	conn *grpc.ClientConn
	kv   pb.KVClient
}

func newEtcdMetadata(cfg agro.Config) (agro.MetadataService, error) {
	conn, err := grpc.Dial(cfg.MetadataAddress)
	if err != nil {
		return nil, err
	}
	client := pb.NewKVClient(conn)

	//TODO(barakmich): Fetch global metadata. If it doesn't exist, we haven't run mkfs yet. For now, we'll just hardcode it.
	global := agro.GlobalMetadata{
		BlockSize:        8 * 1024,
		DefaultBlockSpec: agro.BlockLayerSpec{blockset.CRC, blockset.Base},
	}
	return &etcd{
		cfg:    cfg,
		global: global,
		conn:   conn,
		kv:     client,
	}, nil
}

func (e *etcd) GlobalMetadata() (agro.GlobalMetadata, error) {
	return e.global, nil
}

func (e *etcd) Close() error {
	return e.conn.Close()
}

func (e *etcd) CreateVolume(volume string) error {
	tx := tx().If(
		keyEquals(mkKey("meta", "volumeprinter"), uint64ToBytes(e.volumeprinter)),
	).Then(
		setKey(mkKey("meta", "volumeprinter"), uint64ToBytes(e.volumeprinter+1)),
		setKey(mkKey("volumes", volume), uint64ToBytes(e.volumeprinter)),
	).Tx()
	resp, err := e.kv.Txn(context.Background(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrAgain
	}
	e.volumeprinter++
	return nil
}

func (e *etcd) GetVolumes() ([]string, error) {
	req := getPrefix(mkKey("volumes"))
	resp, err := e.kv.Range(context.Background(), req)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, x := range resp.GetKvs() {
		p := string(x.Key)
		out = append(out, path.Base(p))
	}
	return out, nil
}

func (e *etcd) GetVolumeID(volume string) (agro.VolumeID, error) {
	if v, ok := e.volumesCache[volume]; ok {
		return v, nil
	}
	req := getKey(mkKey("volumes", volume))
	resp, err := e.kv.Range(context.Background(), req)
	if err != nil {
		return 0, err
	}
}
