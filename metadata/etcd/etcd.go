package etcd

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path"
	"sync"
	"time"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/metadata"
	"github.com/barakmich/agro/models"
	"github.com/barakmich/agro/ring"

	"github.com/coreos/pkg/capnslog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	// TODO(barakmich): And this is why vendoring sucks. I shouldn't need to
	// do this, but we're vendoring the etcd proto definitions by hand. The alternative
	// is to use *etcds vendored* version of grpc and net/context everywhere, which is
	// horrifying. This might be helped by GO15VENDORING but we'll see.
	pb "github.com/barakmich/agro/internal/etcdproto/etcdserverpb"
)

var clog = capnslog.NewPackageLogger("github.com/barakmich/agro", "etcd")

const (
	keyPrefix      = "/github.com/barakmich/agro/"
	peerTimeoutMax = 5 * time.Second
)

func init() {
	agro.RegisterMetadataService("etcd", newEtcdMetadata)
	agro.RegisterMkfs("etcd", mkfs)
	agro.RegisterSetRing("etcd", setRing)
}

type etcdCtx struct {
	etcd *etcd
	ctx  context.Context
}

type etcd struct {
	etcdCtx
	mut          sync.RWMutex
	cfg          agro.Config
	global       agro.GlobalMetadata
	volumeminter agro.VolumeID
	inodeminter  agro.INodeID
	volumesCache map[string]agro.VolumeID

	ringListeners []chan agro.Ring

	conn *grpc.ClientConn
	kv   pb.KVClient
	uuid string
}

func newEtcdMetadata(cfg agro.Config) (agro.MetadataService, error) {
	uuid, err := metadata.MakeOrGetUUID(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(cfg.MetadataAddress, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := pb.NewKVClient(conn)

	e := &etcd{
		cfg:          cfg,
		conn:         conn,
		kv:           client,
		volumesCache: make(map[string]agro.VolumeID),
		uuid:         uuid,
	}
	e.etcdCtx.etcd = e
	err = e.getGlobalMetadata()
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (e *etcd) Close() error {
	for _, l := range e.ringListeners {
		close(l)
	}
	return e.conn.Close()
}

func (e *etcd) getGlobalMetadata() error {
	tx := tx().If(
		keyExists(mkKey("meta", "globalmetadata")),
	).Then(
		getKey(mkKey("meta", "volumeminter")),
		getKey(mkKey("meta", "inodeminter")),
		getKey(mkKey("meta", "globalmetadata")),
	).Tx()
	resp, err := e.kv.Txn(context.Background(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrNoGlobalMetadata
	}

	e.volumeminter = agro.VolumeID(bytesToUint64(resp.Responses[0].GetResponseRange().Kvs[0].Value))
	e.inodeminter = agro.INodeID(bytesToUint64(resp.Responses[1].GetResponseRange().Kvs[0].Value))

	var gmd agro.GlobalMetadata
	err = json.Unmarshal(resp.Responses[2].GetResponseRange().Kvs[0].Value, &gmd)
	if err != nil {
		return err
	}
	e.global = gmd
	return nil
}

func (e *etcd) WithContext(ctx context.Context) agro.MetadataService {
	return &etcdCtx{
		etcd: e,
		ctx:  ctx,
	}
}

func (e *etcd) SubscribeNewRings(ch chan agro.Ring) {
	e.mut.Lock()
	defer e.mut.Unlock()
	e.ringListeners = append(e.ringListeners, ch)
}

// Context-sensitive calls

func (c *etcdCtx) getContext() context.Context {
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

func (c *etcdCtx) WithContext(ctx context.Context) agro.MetadataService {
	return c.etcd.WithContext(ctx)
}

func (c *etcdCtx) Close() error {
	return c.etcd.Close()
}

func (c *etcdCtx) GlobalMetadata() (agro.GlobalMetadata, error) {
	return c.etcd.global, nil
}

func (c *etcdCtx) UUID() string {
	return c.etcd.uuid
}

func (c *etcdCtx) RegisterPeer(p *models.PeerInfo) error {
	p.LastSeen = time.Now().UnixNano()
	data, err := p.Marshal()
	if err != nil {
		return err
	}
	_, err = c.etcd.kv.Put(c.getContext(),
		setKey(mkKey("nodes", p.UUID), data),
	)
	return err
}

func (c *etcdCtx) GetPeers() ([]*models.PeerInfo, error) {
	resp, err := c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("nodes")))
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

func (c *etcdCtx) CreateVolume(volume string) error {
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	key := agro.Path{Volume: volume, Path: "/"}
	tx := tx().If(
		keyEquals(mkKey("meta", "volumeminter"), uint64ToBytes(uint64(c.etcd.volumeminter))),
	).Then(
		setKey(mkKey("meta", "volumeminter"), uint64ToBytes(uint64(c.etcd.volumeminter+1))),
		setKey(mkKey("volumes", volume), uint64ToBytes(uint64(c.etcd.volumeminter+1))),
		setKey(mkKey("dirs", key.Key()), newDirProto(&models.Metadata{})),
	).Else(
		getKey(mkKey("meta", "volumeminter")),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		c.etcd.volumeminter = agro.VolumeID(bytesToUint64(resp.Responses[0].GetResponseRange().Kvs[0].Value))
		return agro.ErrAgain
	}
	c.etcd.volumeminter++
	return nil
}

func (c *etcdCtx) GetVolumes() ([]string, error) {
	resp, err := c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumes")))
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

func (c *etcdCtx) GetVolumeID(volume string) (agro.VolumeID, error) {
	if v, ok := c.etcd.volumesCache[volume]; ok {
		return v, nil
	}
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	req := getKey(mkKey("volumes", volume))
	resp, err := c.etcd.kv.Range(c.getContext(), req)
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
	c.etcd.volumesCache[volume] = vid
	return vid, nil

}

func (c *etcdCtx) CommitInodeIndex() (agro.INodeID, error) {
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	tx := tx().If(
		keyEquals(mkKey("meta", "inodeminter"), uint64ToBytes(uint64(c.etcd.inodeminter))),
	).Then(
		setKey(mkKey("meta", "inodeminter"), uint64ToBytes(uint64(c.etcd.inodeminter+1))),
	).Else(
		getKey(mkKey("meta", "inodeminter")),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if err != nil {
		return 0, err
	}
	if !resp.Succeeded {
		c.etcd.inodeminter = agro.INodeID(bytesToUint64(resp.Responses[0].GetResponseRange().Kvs[0].Value))
		return 0, agro.ErrAgain
	}
	i := c.etcd.inodeminter
	c.etcd.inodeminter++
	return i, nil
}

func (c *etcdCtx) Mkdir(path agro.Path, dir *models.Directory) error {
	super, ok := path.Super()
	if !ok {
		return errors.New("etcd: not a directory")
	}
	tx := tx().If(
		keyExists(mkKey("dirs", super.Key())),
	).Then(
		setKey(mkKey("dirs", path.Key()), newDirProto(&models.Metadata{})),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return os.ErrNotExist
	}
	return nil
}

func (c *etcdCtx) getDirRaw(p agro.Path) (*pb.TxnResponse, error) {
	tx := tx().If(
		keyExists(mkKey("dirs", p.Key())),
	).Then(
		getKey(mkKey("dirs", p.Key())),
		getPrefix(mkKey("dirs", p.SubdirsPrefix())),
	).Tx()
	return c.etcd.kv.Txn(c.getContext(), tx)
}

func (c *etcdCtx) Getdir(p agro.Path) (*models.Directory, []agro.Path, error) {
	resp, err := c.getDirRaw(p)
	if err != nil {
		return nil, nil, err
	}
	if !resp.Succeeded {
		return nil, nil, os.ErrNotExist
	}
	dirkv := resp.Responses[0].GetResponseRange().Kvs[0]
	outdir := &models.Directory{}
	err = outdir.Unmarshal(dirkv.Value)
	if err != nil {
		return nil, nil, err
	}
	var outpaths []agro.Path
	for _, kv := range resp.Responses[1].GetResponseRange().Kvs {
		s := bytes.SplitN(kv.Key, []byte{':'}, 2)
		outpaths = append(outpaths, agro.Path{
			Volume: p.Volume,
			Path:   string(s[2]),
		})
	}
	return outdir, outpaths, nil
}

func (c *etcdCtx) SetFileINode(p agro.Path, ref agro.INodeRef) error {
	resp, err := c.getDirRaw(p)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		panic("shouldn't be able to SetFileINode a non-existent dir")
	}
	return c.trySetFileINode(p, ref, resp)
}

func (c *etcdCtx) trySetFileINode(p agro.Path, ref agro.INodeRef, resp *pb.TxnResponse) error {
	dirkv := resp.Responses[0].GetResponseRange().Kvs[0]
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
	resp, err = c.etcd.kv.Txn(c.getContext(), tx)
	if err != nil {
		return err
	}
	if resp.Succeeded {
		return nil
	}
	return c.trySetFileINode(p, ref, resp)
}

func (c *etcdCtx) GetRing() (agro.Ring, error) {
	resp, err := c.etcd.kv.Range(c.getContext(), getKey(mkKey("meta", "the-one-ring")))
	if err != nil {
		return nil, err
	}
	return ring.Unmarshal(resp.Kvs[0].Value)
}

func (c *etcdCtx) SubscribeNewRings(ch chan agro.Ring) {
	c.etcd.SubscribeNewRings(ch)
}
