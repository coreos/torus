package etcd

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/metadata"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"

	"github.com/coreos/pkg/capnslog"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	// TODO(barakmich): And this is why vendoring sucks. I shouldn't need to
	// do this, but we're vendoring the etcd proto definitions by hand. The alternative
	// is to use *etcds vendored* version of grpc and net/context everywhere, which is
	// horrifying. This might be helped by GO15VENDORING but we'll see.
	etcdpb "github.com/coreos/agro/internal/etcdproto/etcdserverpb"
)

// Package rule for etcd keys: always put the static parts first, followed by
// the variables. This makes range gets a lot easier.

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "etcd")

const (
	KeyPrefix      = "/github.com/coreos/agro/"
	peerTimeoutMax = 50 * time.Second
)

var (
	promAtomicRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_etcd_atomic_retries",
		Help: "Number of times an atomic update failed and needed to be retried",
	}, []string{"key"})
	promOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_etcd_base_ops_total",
		Help: "Number of times an atomic update failed and needed to be retried",
	}, []string{"kind"})
)

func init() {
	agro.RegisterMetadataService("etcd", newEtcdMetadata)
	agro.RegisterMkfs("etcd", mkfs)
	agro.RegisterSetRing("etcd", setRing)

	prometheus.MustRegister(promAtomicRetries)
	prometheus.MustRegister(promOps)
}

type etcdCtx struct {
	etcd *Etcd
	ctx  context.Context
}

type Etcd struct {
	etcdCtx
	mut          sync.RWMutex
	cfg          agro.Config
	global       agro.GlobalMetadata
	volumesCache map[string]*models.Volume

	ringListeners []chan agro.Ring

	conn *grpc.ClientConn
	KV   etcdpb.KVClient

	leaseclient etcdpb.LeaseClient
	leasestream etcdpb.Lease_LeaseKeepAliveClient
	uuid        string
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
	client := etcdpb.NewKVClient(conn)

	e := &Etcd{
		cfg:          cfg,
		conn:         conn,
		KV:           client,
		volumesCache: make(map[string]*models.Volume),
		uuid:         uuid,
	}
	e.etcdCtx.etcd = e
	err = e.getGlobalMetadata()
	if err != nil {
		return nil, err
	}
	err = e.watchRingUpdates()
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (e *etcdCtx) Kind() agro.MetadataKind {
	return agro.EtcdMetadata
}

func (e *Etcd) Close() error {
	for _, l := range e.ringListeners {
		close(l)
	}
	return e.conn.Close()
}

func (e *Etcd) getGlobalMetadata() error {
	tx := Tx().If(
		KeyExists(MkKey("meta", "globalmetadata")),
	).Then(
		GetKey(MkKey("meta", "globalmetadata")),
	).Tx()
	resp, err := e.KV.Txn(context.Background(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrNoGlobalMetadata
	}

	var gmd agro.GlobalMetadata
	err = json.Unmarshal(resp.Responses[0].GetResponseRange().Kvs[0].Value, &gmd)
	if err != nil {
		return err
	}
	e.global = gmd
	return nil
}

func (e *Etcd) WithContext(ctx context.Context) agro.MetadataService {
	return &etcdCtx{
		etcd: e,
		ctx:  ctx,
	}
}

func (e *Etcd) SubscribeNewRings(ch chan agro.Ring) {
	e.mut.Lock()
	defer e.mut.Unlock()
	e.ringListeners = append(e.ringListeners, ch)
}

func (e *Etcd) UnsubscribeNewRings(ch chan agro.Ring) {
	e.mut.Lock()
	defer e.mut.Unlock()
	for i, c := range e.ringListeners {
		if ch == c {
			e.ringListeners = append(e.ringListeners[:i], e.ringListeners[i+1:]...)
		}
	}
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

func (c *etcdCtx) RegisterPeer(lease int64, p *models.PeerInfo) error {
	if lease == 0 {
		return errors.New("no lease")
	}
	promOps.WithLabelValues("register-peer").Inc()
	p.LastSeen = time.Now().UnixNano()
	data, err := p.Marshal()
	if err != nil {
		return err
	}
	if c.etcd.leasestream != nil {
		c.etcd.leasestream.Send(&etcdpb.LeaseKeepAliveRequest{
			ID: lease,
		})
		resp, err := c.etcd.leasestream.Recv()
		if err != nil {
			return err
		}
		clog.Tracef("updated lease for %d, TTL %d", resp.ID, resp.TTL)
	}
	_, err = c.etcd.KV.Put(c.getContext(),
		SetLeasedKey(lease, MkKey("nodes", p.UUID), data),
	)
	return err
}

func (c *etcdCtx) GetPeers() (agro.PeerInfoList, error) {
	promOps.WithLabelValues("get-peers").Inc()
	resp, err := c.etcd.KV.Range(c.getContext(), GetPrefix(MkKey("nodes")))
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
			clog.Warningf("peer at key %s didn't unregister; should be fixed with leases in etcdv3", string(x.Key))
			continue
		}
		out = append(out, &p)
	}
	return agro.PeerInfoList(out), nil
}

// AtomicModifyFunc is a class of commutative functions that, given the current
// state of a key's value `in`, returns the new state of the key `out`, and
// `data` to be returned to the calling function on success, or an `err`.
//
// This function may be run mulitple times, if the value has changed in the time
// between getting the data and setting the new value.
type AtomicModifyFunc func(in []byte) (out []byte, data interface{}, err error)

func (c *etcdCtx) AtomicModifyKey(key []byte, f AtomicModifyFunc) (interface{}, error) {
	resp, err := c.etcd.KV.Range(c.getContext(), GetKey(key))
	if err != nil {
		return nil, err
	}
	var version int64
	var value []byte
	if len(resp.Kvs) != 1 {
		version = 0
		value = []byte{}
	} else {
		kv := resp.Kvs[0]
		version = kv.Version
		value = kv.Value
	}
	for {
		newBytes, fval, err := f(value)
		if err != nil {
			return nil, err
		}
		tx := Tx().If(
			KeyIsVersion(key, version),
		).Then(
			SetKey(key, newBytes),
		).Else(
			GetKey(key),
		).Tx()
		resp, err := c.etcd.KV.Txn(c.getContext(), tx)
		if err != nil {
			return nil, err
		}
		if resp.Succeeded {
			return fval, nil
		}
		promAtomicRetries.WithLabelValues(string(key)).Inc()
		kv := resp.Responses[0].GetResponseRange().Kvs[0]
		version = kv.Version
		value = kv.Value
	}
}

func BytesAddOne(in []byte) ([]byte, interface{}, error) {
	newval := BytesToUint64(in) + 1
	return Uint64ToBytes(newval), newval, nil
}

func (c *etcdCtx) GetVolumes() ([]*models.Volume, error) {
	promOps.WithLabelValues("get-volumes").Inc()
	resp, err := c.etcd.KV.Range(c.getContext(), GetPrefix(MkKey("volumeid")))
	if err != nil {
		return nil, err
	}
	var out []*models.Volume
	for _, x := range resp.Kvs {
		v := &models.Volume{}
		err := v.Unmarshal(x.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func (c *etcdCtx) GetVolume(volume string) (*models.Volume, error) {
	if v, ok := c.etcd.volumesCache[volume]; ok {
		return v, nil
	}
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	req := GetKey(MkKey("volumes", volume))
	resp, err := c.etcd.KV.Range(c.getContext(), req)
	if err != nil {
		return nil, err
	}
	if resp.More {
		// What do?
		return nil, errors.New("implement me")
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.New("etcd: no such volume exists")
	}
	vid := BytesToUint64(resp.Kvs[0].Value)
	resp, err = c.etcd.KV.Range(c.getContext(), GetKey(MkKey("volumeid", Uint64ToHex(vid))))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.New("etcd: no such volume ID exists")
	}
	v := &models.Volume{}
	err = v.Unmarshal(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}
	c.etcd.volumesCache[volume] = v
	return v, nil
}

func (c *etcdCtx) GetLease() (int64, error) {
	var err error
	if c.etcd.leaseclient == nil {
		c.etcd.leaseclient = etcdpb.NewLeaseClient(c.etcd.conn)
		c.etcd.leasestream, err = c.etcd.leaseclient.LeaseKeepAlive(context.TODO())
		if err != nil {
			return 0, err
		}
	}
	resp, err := c.etcd.leaseclient.LeaseCreate(c.getContext(), &etcdpb.LeaseCreateRequest{
		TTL: 30,
	})
	if err != nil {
		return 0, err
	}
	return resp.ID, nil
}

func (c *etcdCtx) GetRing() (agro.Ring, error) {
	promOps.WithLabelValues("get-ring").Inc()
	resp, err := c.etcd.KV.Range(c.getContext(), GetKey(MkKey("meta", "the-one-ring")))
	if err != nil {
		return nil, err
	}
	return ring.Unmarshal(resp.Kvs[0].Value)
}

func (c *etcdCtx) SubscribeNewRings(ch chan agro.Ring) {
	c.etcd.SubscribeNewRings(ch)
}

func (c *etcdCtx) UnsubscribeNewRings(ch chan agro.Ring) {
	c.etcd.UnsubscribeNewRings(ch)
}

func (c *etcdCtx) SetRing(ring agro.Ring) error {
	b, err := ring.Marshal()
	if err != nil {
		return err
	}
	key := MkKey("meta", "the-one-ring")
	_, err = c.etcd.KV.Put(c.getContext(),
		SetKey(key, b))
	if err != nil {
		return err
	}
	return nil

}

func (c *etcdCtx) CommitINodeIndex(vid agro.VolumeID) (agro.INodeID, error) {
	promOps.WithLabelValues("commit-inode-index").Inc()
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	newID, err := c.AtomicModifyKey(MkKey("volumemeta", "inode", Uint64ToHex(uint64(vid))), BytesAddOne)
	if err != nil {
		return 0, err
	}
	return agro.INodeID(newID.(uint64)), nil
}

func (c *etcdCtx) NewVolumeID() (agro.VolumeID, error) {
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	newID, err := c.AtomicModifyKey(MkKey("meta", "volumeminter"), BytesAddOne)
	if err != nil {
		return 0, err
	}
	return agro.VolumeID(newID.(uint64)), nil
}

func (c *etcdCtx) GetINodeIndex(vid agro.VolumeID) (agro.INodeID, error) {
	promOps.WithLabelValues("get-inode-index").Inc()
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	resp, err := c.etcd.KV.Range(c.getContext(), GetKey(MkKey("volumemeta", "inode", Uint64ToHex(uint64(vid)))))
	if err != nil {
		return agro.INodeID(0), err
	}
	if len(resp.Kvs) != 1 {
		return agro.INodeID(0), agro.ErrNotExist
	}
	id := BytesToUint64(resp.Kvs[0].Value)
	return agro.INodeID(id), nil
}
