package etcd

import (
	"github.com/barakmich/agro"
	"github.com/barakmich/agro/blockset"

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
	cfg    agro.Config
	global agro.GlobalMetadata

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
	tx := pb.TxnRequest{}
}
