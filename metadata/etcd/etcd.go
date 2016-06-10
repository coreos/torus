package etcd

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/coreos/torus"
	"github.com/coreos/torus/metadata"
	"github.com/coreos/torus/models"
	"github.com/coreos/torus/ring"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/pkg/capnslog"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
)

// Package rule for etcd keys: always put the static parts first, followed by
// the variables. This makes range gets a lot easier.

var clog = capnslog.NewPackageLogger("github.com/coreos/torus", "etcd")

const (
	KeyPrefix      = "/github.com/coreos/torus/"
	peerTimeoutMax = 50 * time.Second
)

var (
	promAtomicRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "torus_etcd_atomic_retries",
		Help: "Number of times an atomic update failed and needed to be retried",
	}, []string{"key"})
	promOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "torus_etcd_base_ops_total",
		Help: "Number of times an atomic update failed and needed to be retried",
	}, []string{"kind"})
)

func init() {
	torus.RegisterMetadataService("etcd", newEtcdMetadata)
	torus.RegisterMetadataInit("etcd", initEtcdMetadata)
	torus.RegisterMetadataWipe("etcd", wipeEtcdMetadata)
	torus.RegisterSetRing("etcd", setRing)

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
	cfg          torus.Config
	global       torus.GlobalMetadata
	volumesCache map[string]*models.Volume

	ringListeners []chan torus.Ring

	Client *etcdv3.Client

	uuid string
}

func newEtcdMetadata(cfg torus.Config) (torus.MetadataService, error) {
	uuid, err := metadata.MakeOrGetUUID(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	v3cfg := etcdv3.Config{Endpoints: []string{cfg.MetadataAddress}}
	client, err := etcdv3.New(v3cfg)
	if err != nil {
		return nil, err
	}

	e := &Etcd{
		cfg:          cfg,
		Client:       client,
		volumesCache: make(map[string]*models.Volume),
		uuid:         uuid,
	}
	e.etcdCtx.etcd = e
	err = e.getGlobalMetadata()
	if err != nil {
		return nil, err
	}
	if err = e.watchRingUpdates(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *etcdCtx) Kind() torus.MetadataKind {
	return torus.EtcdMetadata
}

func (e *Etcd) Close() error {
	for _, l := range e.ringListeners {
		close(l)
	}
	return e.Client.Close()
}

func (e *Etcd) getGlobalMetadata() error {
	txn := e.Client.Txn(context.Background())
	resp, err := txn.If(
		etcdv3.Compare(etcdv3.Version(MkKey("meta", "globalmetadata")), ">", 0),
	).Then(
		etcdv3.OpGet(MkKey("meta", "globalmetadata")),
	).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return torus.ErrNoGlobalMetadata
	}

	var gmd torus.GlobalMetadata
	err = json.Unmarshal(resp.Responses[0].GetResponseRange().Kvs[0].Value, &gmd)
	if err != nil {
		return err
	}
	e.global = gmd
	return nil
}

func (e *Etcd) WithContext(ctx context.Context) torus.MetadataService {
	return &etcdCtx{
		etcd: e,
		ctx:  ctx,
	}
}

func (e *Etcd) SubscribeNewRings(ch chan torus.Ring) {
	e.mut.Lock()
	defer e.mut.Unlock()
	e.ringListeners = append(e.ringListeners, ch)
}

func (e *Etcd) UnsubscribeNewRings(ch chan torus.Ring) {
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

func (c *etcdCtx) WithContext(ctx context.Context) torus.MetadataService {
	return c.etcd.WithContext(ctx)
}

func (c *etcdCtx) Close() error {
	return c.etcd.Close()
}

func (c *etcdCtx) GlobalMetadata() (torus.GlobalMetadata, error) {
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

	lid := etcdv3.LeaseID(lease)
	resp, err := c.etcd.Client.KeepAliveOnce(c.getContext(), lid)
	if err != nil {
		return err
	}
	clog.Tracef("updated lease for %d, TTL %d", resp.ID, resp.TTL)
	_, err = c.etcd.Client.Put(
		c.getContext(), MkKey("nodes", p.UUID), string(data), etcdv3.WithLease(lid))
	return err
}

func (c *etcdCtx) GetPeers() (torus.PeerInfoList, error) {
	promOps.WithLabelValues("get-peers").Inc()
	resp, err := c.etcd.Client.Get(c.getContext(), MkKey("nodes"), etcdv3.WithPrefix())
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
	return torus.PeerInfoList(out), nil
}

// AtomicModifyFunc is a class of commutative functions that, given the current
// state of a key's value `in`, returns the new state of the key `out`, and
// `data` to be returned to the calling function on success, or an `err`.
//
// This function may be run multiple times, if the value has changed in the time
// between getting the data and setting the new value.
type AtomicModifyFunc func(in []byte) (out []byte, data interface{}, err error)

func (c *etcdCtx) AtomicModifyKey(k []byte, f AtomicModifyFunc) (interface{}, error) {
	key := string(k)
	resp, err := c.etcd.Client.Get(c.getContext(), key)
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
		txn := c.etcd.Client.Txn(c.getContext()).If(
			etcdv3.Compare(etcdv3.Version(key), "=", version),
		).Then(
			etcdv3.OpPut(key, string(newBytes)),
		).Else(
			etcdv3.OpGet(key),
		)
		resp, err := txn.Commit()
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

func (c *etcdCtx) GetVolumes() ([]*models.Volume, torus.VolumeID, error) {
	promOps.WithLabelValues("get-volumes").Inc()
	txn := c.etcd.Client.Txn(c.getContext()).Then(
		etcdv3.OpGet(MkKey("meta", "volumeminter")),
		etcdv3.OpGet(MkKey("volumeid"), etcdv3.WithPrefix()),
	)
	resp, err := txn.Commit()
	if err != nil {
		return nil, 0, err
	}
	highwater := BytesToUint64(resp.Responses[0].GetResponseRange().Kvs[0].Value)
	list := resp.Responses[1].GetResponseRange().Kvs
	var out []*models.Volume
	for _, x := range list {
		v := &models.Volume{}
		err := v.Unmarshal(x.Value)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, v)
	}
	return out, torus.VolumeID(highwater), nil
}

func (c *etcdCtx) GetVolume(volume string) (*models.Volume, error) {
	if v, ok := c.etcd.volumesCache[volume]; ok {
		return v, nil
	}
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	resp, err := c.etcd.Client.Get(c.getContext(), MkKey("volumes", volume))
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
	resp, err = c.etcd.Client.Get(c.getContext(), MkKey("volumeid", Uint64ToHex(vid)))
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
	resp, err := c.etcd.Client.Grant(c.getContext(), 30)
	if err != nil {
		return 0, err
	}
	return int64(resp.ID), nil
}

func (c *etcdCtx) GetRing() (torus.Ring, error) {
	r, _, err := c.getRing()
	return r, err
}
func (c *etcdCtx) getRing() (torus.Ring, int64, error) {
	promOps.WithLabelValues("get-ring").Inc()
	resp, err := c.etcd.Client.Get(c.getContext(), MkKey("meta", "the-one-ring"))
	if err != nil {
		return nil, 0, err
	}
	if len(resp.Kvs) == 0 {
		return nil, 0, torus.ErrNoGlobalMetadata
	}
	ring, err := ring.Unmarshal(resp.Kvs[0].Value)
	if err != nil {
		return nil, 0, err
	}
	return ring, resp.Kvs[0].Version, nil
}

func (c *etcdCtx) SubscribeNewRings(ch chan torus.Ring) {
	c.etcd.SubscribeNewRings(ch)
}

func (c *etcdCtx) UnsubscribeNewRings(ch chan torus.Ring) {
	c.etcd.UnsubscribeNewRings(ch)
}

func (c *etcdCtx) SetRing(ring torus.Ring) error {
	oldr, etcdver, err := c.getRing()
	if oldr.Version() != ring.Version()-1 {
		return torus.ErrNonSequentialRing
	}
	b, err := ring.Marshal()
	if err != nil {
		return err
	}
	key := MkKey("meta", "the-one-ring")
	txn := c.etcd.Client.Txn(c.getContext()).If(
		etcdv3.Compare(etcdv3.Version(key), "=", etcdver),
	).Then(
		etcdv3.OpPut(key, string(b)),
	)
	resp, err := txn.Commit()
	if err != nil {
		return err
	}
	if resp.Succeeded {
		return nil
	}
	return torus.ErrNonSequentialRing
}

func (c *etcdCtx) CommitINodeIndex(vid torus.VolumeID) (torus.INodeID, error) {
	promOps.WithLabelValues("commit-inode-index").Inc()
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	k := []byte(MkKey("volumemeta", Uint64ToHex(uint64(vid)), "inode"))
	newID, err := c.AtomicModifyKey(k, BytesAddOne)
	if err != nil {
		return 0, err
	}
	return torus.INodeID(newID.(uint64)), nil
}

func (c *etcdCtx) NewVolumeID() (torus.VolumeID, error) {
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	k := []byte(MkKey("meta", "volumeminter"))
	newID, err := c.AtomicModifyKey(k, BytesAddOne)
	if err != nil {
		return 0, err
	}
	return torus.VolumeID(newID.(uint64)), nil
}

func (c *etcdCtx) GetINodeIndex(vid torus.VolumeID) (torus.INodeID, error) {
	promOps.WithLabelValues("get-inode-index").Inc()
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	resp, err := c.etcd.Client.Get(c.getContext(), MkKey("volumemeta", Uint64ToHex(uint64(vid)), "inode"))
	if err != nil {
		return torus.INodeID(0), err
	}
	if len(resp.Kvs) != 1 {
		return torus.INodeID(0), torus.ErrNotExist
	}
	id := BytesToUint64(resp.Kvs[0].Value)
	return torus.INodeID(id), nil
}
