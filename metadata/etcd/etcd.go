package etcd

import (
	"bytes"
	"errors"
	"os"
	"path"
	"time"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/blockset"
	"github.com/barakmich/agro/metadata"
	"github.com/barakmich/agro/models"
	"golang.org/x/net/context"

	// TODO(barakmich): And this is why vendoring sucks. I shouldn't need to
	//import this, but I do, because I'm using etcdserverpb from head, and *it*
	//expects its own vendored version. Admittedly, this should get better with
	//GO15VENDORING, but etcd doesn't support that yet.
	"github.com/coreos/etcd/Godeps/_workspace/src/google.golang.org/grpc"
	"github.com/coreos/pkg/capnslog"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

var clog = capnslog.NewPackageLogger("github.com/barakmich/agro", "etcd")

const (
	keyPrefix      = "/github.com/barakmich/agro/"
	peerTimeoutMax = 5 * time.Second
)

func init() {
	agro.RegisterMetadataService("etcd", newEtcdMetadata)
}

type etcd struct {
	cfg           agro.Config
	global        agro.GlobalMetadata
	volumeprinter agro.VolumeID
	inodeprinter  agro.INodeID
	volumesCache  map[string]agro.VolumeID

	conn *grpc.ClientConn
	kv   pb.KVClient
	uuid string
}

func newEtcdMetadata(cfg agro.Config) (agro.MetadataService, error) {
	uuid, err := metadata.MakeOrGetUUID(cfg.DataDir)
	if err != nil {
		return nil, err
	}

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
		cfg:          cfg,
		global:       global,
		conn:         conn,
		kv:           client,
		volumesCache: make(map[string]agro.VolumeID),
		uuid:         uuid,
	}, nil
}

func (e *etcd) GlobalMetadata() (agro.GlobalMetadata, error) {
	return e.global, nil
}

func (e *etcd) UUID() string {
	return e.uuid
}

func (e *etcd) RegisterPeer(p *models.PeerInfo) error {
	p.LastSeen = time.Now().UnixNano()
	data, err := p.Marshal()
	if err != nil {
		return err
	}
	_, err = e.kv.Put(context.Background(),
		setKey(mkKey("nodes", p.UUID), data),
	)
	return err
}

func (e *etcd) GetPeers() ([]*models.PeerInfo, error) {
	resp, err := e.kv.Range(context.Background(), getPrefix(mkKey("nodes")))
	if err != nil {
		return nil, err
	}
	var out []*models.PeerInfo
	for _, x := range resp.Kvs {
		var p models.PeerInfo
		err := p.Unmarshal(x.Value)
		if err != nil {
			// Intentionally ignore a peer that doesn't unmarshal properly.
			clog.Errorf("peer at key %s didn't unmarshal correctly", string(x.Key))
			continue
		}
		if time.Since(time.Unix(0, p.LastSeen)) > peerTimeoutMax {
			clog.Debugf("peer at key %s didn't unregister; fixed with leases in etcdv3", string(x.Key))
			continue
		}
		out = append(out, &p)
	}
	return out, nil
}

func (e *etcd) Close() error {
	return e.conn.Close()
}

func (e *etcd) CreateVolume(volume string) error {
	key := agro.Path{Volume: volume, Path: "/"}
	tx := tx().If(
		keyEquals(mkKey("meta", "volumeprinter"), uint64ToBytes(uint64(e.volumeprinter))),
	).Then(
		setKey(mkKey("meta", "volumeprinter"), uint64ToBytes(uint64(e.volumeprinter+1))),
		setKey(mkKey("volumes", volume), uint64ToBytes(uint64(e.volumeprinter+1))),
		setKey(mkKey("dirs", key.Key()), newDirProto(&models.Metadata{})),
	).Else(
		getKey(mkKey("meta", "volumeprinter")),
	).Tx()
	resp, err := e.kv.Txn(context.Background(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		e.volumeprinter = agro.VolumeID(bytesToUint64(resp.Responses[0].ResponseRange.Kvs[0].Value))
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
	for _, x := range resp.Kvs {
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
	if resp.More {
		// What do?
		return 0, errors.New("implement me")
	}
	if len(resp.Kvs) == 0 {
		return 0, errors.New("etcd: no such volume exists")
	}
	vid := agro.VolumeID(bytesToUint64(resp.Kvs[0].Value))
	e.volumesCache[volume] = vid
	return vid, nil

}

func (e *etcd) CommitInodeIndex() (agro.INodeID, error) {
	tx := tx().If(
		keyEquals(mkKey("meta", "inodeprinter"), uint64ToBytes(uint64(e.inodeprinter))),
	).Then(
		setKey(mkKey("meta", "inodeprinter"), uint64ToBytes(uint64(e.inodeprinter+1))),
	).Else(
		getKey(mkKey("meta", "inodeprinter")),
	).Tx()
	resp, err := e.kv.Txn(context.Background(), tx)
	if err != nil {
		return 0, err
	}
	if !resp.Succeeded {
		e.inodeprinter = agro.INodeID(bytesToUint64(resp.Responses[0].ResponseRange.Kvs[0].Value))
		return 0, agro.ErrAgain
	}
	i := e.inodeprinter
	e.inodeprinter++
	return i, nil
}

func (e *etcd) Mkdir(path agro.Path, dir *models.Directory) error {
	super, ok := path.Super()
	if !ok {
		return errors.New("etcd: not a directory")
	}
	tx := tx().If(
		keyExists(mkKey("dirs", super.Key())),
	).Then(
		setKey(mkKey("dirs", path.Key()), newDirProto(&models.Metadata{})),
	).Tx()
	resp, err := e.kv.Txn(context.Background(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return os.ErrNotExist
	}
	return nil
}

func (e *etcd) getDirRaw(p agro.Path) (*pb.TxnResponse, error) {
	tx := tx().If(
		keyExists(mkKey("dirs", p.Key())),
	).Then(
		getKey(mkKey("dirs", p.Key())),
		getPrefix(mkKey("dirs", p.SubdirsPrefix())),
	).Tx()
	return e.kv.Txn(context.Background(), tx)
}

func (e *etcd) Getdir(p agro.Path) (*models.Directory, []agro.Path, error) {
	resp, err := e.getDirRaw(p)
	if err != nil {
		return nil, nil, err
	}
	if !resp.Succeeded {
		return nil, nil, os.ErrNotExist
	}
	dirkv := resp.Responses[0].ResponseRange.Kvs[0]
	outdir := &models.Directory{}
	err = outdir.Unmarshal(dirkv.Value)
	if err != nil {
		return nil, nil, err
	}
	var outpaths []agro.Path
	for _, kv := range resp.Responses[1].ResponseRange.Kvs {
		s := bytes.SplitN(kv.Key, []byte{':'}, 2)
		outpaths = append(outpaths, agro.Path{
			Volume: p.Volume,
			Path:   string(s[2]),
		})
	}
	return outdir, outpaths, nil
}

func (e *etcd) SetFileINode(p agro.Path, ref agro.INodeRef) error {
	resp, err := e.getDirRaw(p)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		panic("shouldn't be able to SetFileINode a non-existent dir")
	}
	return e.trySetFileINode(p, ref, resp)
}

func (e *etcd) trySetFileINode(p agro.Path, ref agro.INodeRef, resp *pb.TxnResponse) error {
	dirkv := resp.Responses[0].ResponseRange.Kvs[0]
	dir := &models.Directory{}
	err := dir.Unmarshal(dirkv.Value)
	if err != nil {
		return err
	}
	if dir.Files == nil {
		dir.Files = make(map[string]uint64)
	}
	dir.Files[p.Filename()] = uint64(ref.INode)
	b, err := dir.Marshal()
	tx := tx().If(
		keyIsVersion(dirkv.Key, dirkv.Version),
	).Then(
		setKey(dirkv.Key, b),
	).Else(
		getKey(dirkv.Key),
	).Tx()
	resp, err = e.kv.Txn(context.Background(), tx)
	if err != nil {
		return err
	}
	if resp.Succeeded {
		return nil
	}
	return e.trySetFileINode(p, ref, resp)
}

func (e *etcd) Mkfs(gmd agro.GlobalMetadata) error {
	tx := tx().If(
		keyNotExists(mkKey("meta", "volumeprinter")),
	).Then(
		setKey(mkKey("meta", "volumeprinter"), uint64ToBytes(1)),
		setKey(mkKey("meta", "inodeprinter"), uint64ToBytes(1)),
	).Else(
		getKey(mkKey("meta", "volumeprinter")),
		getKey(mkKey("meta", "inodeprinter")),
	).Tx()
	resp, err := e.kv.Txn(context.Background(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		e.volumeprinter = agro.VolumeID(bytesToUint64(resp.Responses[0].ResponseRange.Kvs[0].Value))
		e.inodeprinter = agro.INodeID(bytesToUint64(resp.Responses[1].ResponseRange.Kvs[0].Value))
		return nil
	}
	e.volumeprinter = 1
	e.inodeprinter = 1
	return nil
}
