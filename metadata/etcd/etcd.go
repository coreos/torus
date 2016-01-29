package etcd

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/metadata"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"
	"github.com/tgruben/roaring"

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
	keyPrefix      = "/github.com/coreos/agro/"
	peerTimeoutMax = 50 * time.Second
)

var (
	promAtomicRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_etcd_atomic_retries",
		Help: "Number of times an atomic update failed and needed to be retried",
	}, []string{"key"})
)

func init() {
	agro.RegisterMetadataService("etcd", newEtcdMetadata)
	agro.RegisterMkfs("etcd", mkfs)
	agro.RegisterSetRing("etcd", setRing)

	prometheus.MustRegister(promAtomicRetries)
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
	volumesCache map[string]agro.VolumeID

	ringListeners []chan agro.Ring

	conn *grpc.ClientConn
	kv   etcdpb.KVClient
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
	client := etcdpb.NewKVClient(conn)

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
	err = e.watchRingUpdates()
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
		getKey(mkKey("meta", "globalmetadata")),
	).Tx()
	resp, err := e.kv.Txn(context.Background(), tx)
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

func (e *etcd) UnsubscribeNewRings(ch chan agro.Ring) {
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
			clog.Tracef("peer at key %s didn't unregister; fixed with leases in etcdv3", string(x.Key))
			continue
		}
		out = append(out, &p)
	}
	return out, nil
}

// AtomicModifyFunc is a class of commutative functions that, given the current
// state of a key's value `in`, returns the new state of the key `out`, and
// `data` to be returned to the calling function on success, or an `err`.
//
// This function may be run mulitple times, if the value has changed in the time
// between getting the data and setting the new value.
type AtomicModifyFunc func(in []byte) (out []byte, data interface{}, err error)

func (c *etcdCtx) atomicModifyKey(key []byte, f AtomicModifyFunc) (interface{}, error) {
	resp, err := c.etcd.kv.Range(c.getContext(), getKey(key))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) != 1 {
		return nil, agro.ErrAgain
	}
	kv := resp.Kvs[0]
	for {
		newBytes, fval, err := f(kv.Value)
		if err != nil {
			return nil, err
		}
		tx := tx().If(
			keyIsVersion(key, kv.Version),
		).Then(
			setKey(key, newBytes),
		).Else(
			getKey(key),
		).Tx()
		resp, err := c.etcd.kv.Txn(c.getContext(), tx)
		if err != nil {
			return nil, err
		}
		if resp.Succeeded {
			return fval, nil
		}
		promAtomicRetries.WithLabelValues(string(key)).Inc()
		kv = resp.Responses[0].GetResponseRange().Kvs[0]
	}
}

func bytesAddOne(in []byte) ([]byte, interface{}, error) {
	newval := bytesToUint64(in) + 1
	return uint64ToBytes(newval), newval, nil
}

func (c *etcdCtx) CreateVolume(volume string) error {
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	key := agro.Path{Volume: volume, Path: "/"}
	newID, err := c.atomicModifyKey(mkKey("meta", "volumeminter"), bytesAddOne)
	if err != nil {
		return err
	}
	sID := strconv.FormatUint(newID.(uint64), 10)
	do := tx().Do(
		setKey(mkKey("volumes", volume), uint64ToBytes(newID.(uint64))),
		setKey(mkKey("volumeid", sID), []byte(volume)),
		setKey(mkKey("volumemeta", "inode", volume), uint64ToBytes(1)),
		setKey(mkKey("volumemeta", "deadmap", volume), roaringToBytes(roaring.NewRoaringBitmap())),
		setKey(mkKey("dirs", key.Key()), newDirProto(&models.Metadata{})),
	).Tx()
	_, err = c.etcd.kv.Txn(c.getContext(), do)
	if err != nil {
		return err
	}
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

func (c *etcdCtx) GetVolumeName(vid agro.VolumeID) (string, error) {
	s := strconv.FormatUint(uint64(vid), 10)
	req := getKey(mkKey("volumeid", s))
	resp, err := c.etcd.kv.Range(c.getContext(), req)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", errors.New("etcd: no such volume exists")
	}
	return string(resp.Kvs[0].Value), nil
}

func (c *etcdCtx) CommitINodeIndex(volume string) (agro.INodeID, error) {
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	newID, err := c.atomicModifyKey(mkKey("volumemeta", "inode", volume), bytesAddOne)
	if err != nil {
		return 0, err
	}
	return agro.INodeID(newID.(uint64)), nil
}

func (c *etcdCtx) GetINodeIndex(volume string) (agro.INodeID, error) {
	resp, err := c.etcd.kv.Range(c.getContext(), getKey(mkKey("volumemeta", "inode", volume)))
	if err != nil {
		return agro.INodeID(0), err
	}
	if len(resp.Kvs) != 1 {
		return agro.INodeID(0), agro.ErrNotExist
	}
	id := bytesToUint64(resp.Kvs[0].Value)
	return agro.INodeID(id), nil
}

func (c *etcdCtx) GetINodeIndexes() (map[string]agro.INodeID, error) {
	resp, err := c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumemeta", "inode")))
	if err != nil {
		return nil, err
	}
	out := make(map[string]agro.INodeID)
	for _, kv := range resp.Kvs {
		vol := path.Base(string(kv.Key))
		id := bytesToUint64(kv.Value)
		out[vol] = agro.INodeID(id)
	}
	return out, nil
}

func (c *etcdCtx) Mkdir(path agro.Path, md *models.Metadata) error {
	parent, ok := path.Parent()
	if !ok {
		return errors.New("etcd: not a directory")
	}
	tx := tx().If(
		keyExists(mkKey("dirs", parent.Key())),
	).Then(
		setKey(mkKey("dirs", path.Key()), newDirProto(md)),
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

func (c *etcdCtx) Rmdir(path agro.Path) error {
	if !path.IsDir() {
		return errors.New("etcd: not a directory")
	}
	if path.Path == "/" {
		return errors.New("etcd: cannot delete root directory")
	}
	dir, subdirs, version, err := c.getdir(path)
	if err != nil {
		return err
	}
	if len(dir.Files) == 0 || len(subdirs) != 0 {
		return errors.New("etcd: directory not empty")
	}
	tx := tx().If(
		keyIsVersion(mkKey("dirs", path.Key()), version),
	).Then(
		deleteKey(mkKey("dirs", path.Key())),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if !resp.Succeeded {
		return os.ErrInvalid
	}
	return nil
}

func (c *etcdCtx) Getdir(p agro.Path) (*models.Directory, []agro.Path, error) {
	dir, paths, _, err := c.getdir(p)
	return dir, paths, err
}

func (c *etcdCtx) getdir(p agro.Path) (*models.Directory, []agro.Path, int64, error) {
	tx := tx().If(
		keyExists(mkKey("dirs", p.Key())),
	).Then(
		getKey(mkKey("dirs", p.Key())),
		getPrefix(mkKey("dirs", p.SubdirsPrefix())),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if err != nil {
		return nil, nil, 0, err
	}
	if !resp.Succeeded {
		return nil, nil, 0, os.ErrNotExist
	}
	dirkv := resp.Responses[0].GetResponseRange().Kvs[0]
	outdir := &models.Directory{}
	err = outdir.Unmarshal(dirkv.Value)
	if err != nil {
		return nil, nil, 0, err
	}
	var outpaths []agro.Path
	for _, kv := range resp.Responses[1].GetResponseRange().Kvs {
		s := bytes.SplitN(kv.Key, []byte{':'}, 3)
		outpaths = append(outpaths, agro.Path{
			Volume: p.Volume,
			Path:   string(s[2]) + "/",
		})
	}
	clog.Debugf("outpaths %#v", outpaths)
	return outdir, outpaths, dirkv.Version, nil
}

func (c *etcdCtx) SetFileINode(p agro.Path, ref agro.INodeRef) (agro.INodeID, error) {
	v, err := c.atomicModifyKey(mkKey("dirs", p.Key()), trySetFileINode(p, ref))
	return v.(agro.INodeID), err
}

func trySetFileINode(p agro.Path, ref agro.INodeRef) AtomicModifyFunc {
	return func(in []byte) ([]byte, interface{}, error) {
		dir := &models.Directory{}
		err := dir.Unmarshal(in)
		if err != nil {
			return nil, agro.INodeID(0), err
		}
		if dir.Files == nil {
			dir.Files = make(map[string]uint64)
		}
		old := agro.INodeID(0)
		if v, ok := dir.Files[p.Filename()]; ok {
			old = agro.INodeID(v)
		}
		if ref.INode == 0 && ref.Volume() == 0 {
			delete(dir.Files, p.Filename())
		} else {
			dir.Files[p.Filename()] = uint64(ref.INode)
		}
		bytes, err := dir.Marshal()
		return bytes, old, err
	}
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

func (c *etcdCtx) UnsubscribeNewRings(ch chan agro.Ring) {
	c.etcd.UnsubscribeNewRings(ch)
}

func (c *etcdCtx) GetVolumeLiveness(volume string) (*roaring.RoaringBitmap, []*roaring.RoaringBitmap, error) {
	tx := tx().Do(
		getKey(mkKey("volumemeta", "deadmap", volume)),
		getPrefix(mkKey("volumemeta", "open", volume)),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if err != nil {
		return nil, nil, err
	}
	deadmap := bytesToRoaring(resp.Responses[0].GetResponseRange().Kvs[0].Value)
	var l []*roaring.RoaringBitmap
	for _, x := range resp.Responses[1].GetResponseRange().Kvs {
		l = append(l, bytesToRoaring(x.Value))
	}
	return deadmap, l, nil
}

func (c *etcdCtx) ClaimVolumeINodes(volume string, inodes *roaring.RoaringBitmap) error {
	// TODO(barakmich): LEASE
	key := mkKey("volumemeta", "open", volume, c.UUID())
	if inodes == nil {
		_, err := c.etcd.kv.DeleteRange(c.getContext(), deleteKey(key))
		return err
	}
	data := roaringToBytes(inodes)
	_, err := c.etcd.kv.Put(c.getContext(),
		setKey(key, data),
	)
	return err
}

func (c *etcdCtx) ModifyDeadMap(volume string, live *roaring.RoaringBitmap, dead *roaring.RoaringBitmap) error {
	_, err := c.atomicModifyKey(mkKey("volumemeta", "deadmap", volume), func(b []byte) ([]byte, interface{}, error) {
		bm := bytesToRoaring(b)
		bm.Or(dead)
		bm.AndNot(live)
		return roaringToBytes(bm), nil, nil
	})
	return err
}

func (c *etcdCtx) SetRing(ring agro.Ring, force bool) error {
	b, err := ring.Marshal()
	if err != nil {
		return err
	}
	key := mkKey("meta", "the-new-ring")
	if force {
		key = mkKey("meta", "the-one-ring")
	}
	_, err = c.etcd.kv.Put(c.getContext(),
		setKey(key, b))
	if err != nil {
		return err
	}
	return nil

}
