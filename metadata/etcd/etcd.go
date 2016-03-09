package etcd

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/metadata"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"

	"github.com/RoaringBitmap/roaring"
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
	chainPageSize  = 1000
)

var (
	promAtomicRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_etcd_atomic_retries",
		Help: "Number of times an atomic update failed and needed to be retried",
	}, []string{"key"})
	promOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_etcd_ops_total",
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
	etcd *etcd
	ctx  context.Context
}

type etcd struct {
	etcdCtx
	mut          sync.RWMutex
	cfg          agro.Config
	global       agro.GlobalMetadata
	volumesCache map[string]*models.Volume

	ringListeners []chan agro.Ring

	conn *grpc.ClientConn
	kv   etcdpb.KVClient

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

	e := &etcd{
		cfg:          cfg,
		conn:         conn,
		kv:           client,
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
	_, err = c.etcd.kv.Put(c.getContext(),
		setLeasedKey(lease, mkKey("nodes", p.UUID), data),
	)
	return err
}

func (c *etcdCtx) GetPeers() (agro.PeerInfoList, error) {
	promOps.WithLabelValues("get-peers").Inc()
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

func (c *etcdCtx) atomicModifyKey(key []byte, f AtomicModifyFunc) (interface{}, error) {
	resp, err := c.etcd.kv.Range(c.getContext(), getKey(key))
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
		tx := tx().If(
			keyIsVersion(key, version),
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
		kv := resp.Responses[0].GetResponseRange().Kvs[0]
		version = kv.Version
		value = kv.Value
	}
}

func bytesAddOne(in []byte) ([]byte, interface{}, error) {
	newval := bytesToUint64(in) + 1
	return uint64ToBytes(newval), newval, nil
}

func (c *etcdCtx) CreateVolume(volume *models.Volume) error {
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	switch volume.Type {
	case models.Volume_FILE:
		return c.createFSVol(volume)
	case models.Volume_BLOCK:
		return c.createBlockVol(volume)
	default:
		panic("unknown volume type")
	}
}

func (c *etcdCtx) createFSVol(volume *models.Volume) error {
	key := agro.Path{Volume: volume.Name, Path: "/"}
	new, err := c.atomicModifyKey(mkKey("meta", "volumeminter"), bytesAddOne)
	volume.Id = new.(uint64)
	if err != nil {
		return err
	}
	t := uint64(time.Now().UnixNano())
	vbytes, err := volume.Marshal()
	if err != nil {
		return err
	}
	do := tx().If(
		keyNotExists(mkKey("volumes", volume.Name)),
	).Then(
		setKey(mkKey("volumes", volume.Name), uint64ToBytes(volume.Id)),
		setKey(mkKey("volumeid", uint64ToHex(volume.Id)), vbytes),
		setKey(mkKey("volumemeta", "inode", uint64ToHex(volume.Id)), uint64ToBytes(1)),
		setKey(mkKey("volumemeta", "deadmap", uint64ToHex(volume.Id)), roaringToBytes(roaring.NewBitmap())),
		setKey(mkKey("dirs", key.Key()), newDirProto(&models.Metadata{
			Ctime: t,
			Mtime: t,
			Mode:  uint32(os.ModeDir | 0755),
		})),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), do)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrExists
	}
	return nil
}

func (c *etcdCtx) GetVolumes() ([]*models.Volume, error) {
	promOps.WithLabelValues("get-volumes").Inc()
	resp, err := c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumeid")))
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
	req := getKey(mkKey("volumes", volume))
	resp, err := c.etcd.kv.Range(c.getContext(), req)
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
	vid := bytesToUint64(resp.Kvs[0].Value)
	resp, err = c.etcd.kv.Range(c.getContext(), getKey(mkKey("volumeid", uint64ToHex(vid))))
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

func (c *etcdCtx) CommitINodeIndex(vol string) (agro.INodeID, error) {
	volume, err := c.GetVolume(vol)
	if err != nil {
		return 0, err
	}
	promOps.WithLabelValues("commit-inode-index").Inc()
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	newID, err := c.atomicModifyKey(mkKey("volumemeta", "inode", uint64ToHex(volume.Id)), bytesAddOne)
	if err != nil {
		return 0, err
	}
	return agro.INodeID(newID.(uint64)), nil
}

func (c *etcdCtx) GetINodeIndex(vol string) (agro.INodeID, error) {
	volume, err := c.GetVolume(vol)
	if err != nil {
		return 0, err
	}
	promOps.WithLabelValues("get-inode-index").Inc()
	c.etcd.mut.Lock()
	defer c.etcd.mut.Unlock()
	resp, err := c.etcd.kv.Range(c.getContext(), getKey(mkKey("volumemeta", "inode", uint64ToHex(volume.Id))))
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
	promOps.WithLabelValues("mkdir").Inc()
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

func (c *etcdCtx) ChangeDirMetadata(p agro.Path, md *models.Metadata) error {
	promOps.WithLabelValues("change-dir-metadata").Inc()
	_, err := c.atomicModifyKey(
		mkKey("dirs", p.Key()),
		func(in []byte) ([]byte, interface{}, error) {
			dir := &models.Directory{}
			dir.Unmarshal(in)
			dir.Metadata = md
			b, err := dir.Marshal()
			return b, nil, err
		})
	return err
}

func (c *etcdCtx) Rmdir(path agro.Path) error {
	promOps.WithLabelValues("rmdir").Inc()
	if !path.IsDir() {
		clog.Error("rmdir: not a directory", path)
		return errors.New("etcd: not a directory")
	}
	if path.Path == "/" {
		clog.Error("rmdir: cannot delete root")
		return errors.New("etcd: cannot delete root directory")
	}
	dir, subdirs, version, err := c.getdir(path)
	if err != nil {
		clog.Error("rmdir: getdir err", err)
		return err
	}
	if len(dir.Files) != 0 || len(subdirs) != 0 {
		clog.Error("rmdir: dir not empty", dir, subdirs)
		return errors.New("etcd: directory not empty")
	}
	tx := tx().If(
		keyIsVersion(mkKey("dirs", path.Key()), version),
	).Then(
		deleteKey(mkKey("dirs", path.Key())),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if !resp.Succeeded {
		clog.Error("rmdir: txn failed")
		return os.ErrInvalid
	}
	return nil
}

func (c *etcdCtx) Getdir(p agro.Path) (*models.Directory, []agro.Path, error) {
	dir, paths, _, err := c.getdir(p)
	return dir, paths, err
}

func (c *etcdCtx) getdir(p agro.Path) (*models.Directory, []agro.Path, int64, error) {
	promOps.WithLabelValues("getdir").Inc()
	clog.Tracef("getdir: %s", p.Key())
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
	clog.Tracef("outpaths %#v", outpaths)
	return outdir, outpaths, dirkv.Version, nil
}

func (c *etcdCtx) SetFileEntry(p agro.Path, ent *models.FileEntry) error {
	promOps.WithLabelValues("set-file-entry").Inc()
	_, err := c.atomicModifyKey(mkKey("dirs", p.Key()), trySetFileEntry(p, ent))
	return err
}

func trySetFileEntry(p agro.Path, ent *models.FileEntry) AtomicModifyFunc {
	return func(in []byte) ([]byte, interface{}, error) {
		dir := &models.Directory{}
		err := dir.Unmarshal(in)
		if err != nil {
			return nil, agro.INodeID(0), err
		}
		if dir.Files == nil {
			dir.Files = make(map[string]*models.FileEntry)
		}
		old := &models.FileEntry{}
		if v, ok := dir.Files[p.Filename()]; ok {
			old = v
		}
		if ent.Chain == 0 && ent.Sympath == "" {
			delete(dir.Files, p.Filename())
		} else {
			dir.Files[p.Filename()] = ent
		}
		bytes, err := dir.Marshal()
		return bytes, old, err
	}
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

func (c *etcdCtx) GetChainINode(base agro.INodeRef) (agro.INodeRef, error) {
	pageID := uint64ToHex(uint64(base.INode / chainPageSize))
	volume := uint64ToHex(uint64(base.Volume()))
	resp, err := c.etcd.kv.Range(c.getContext(), getKey(mkKey("volumemeta", "chain", volume, pageID)))
	if len(resp.Kvs) == 0 {
		return agro.INodeRef{}, nil
	}
	set := &models.FileChainSet{}
	err = set.Unmarshal(resp.Kvs[0].Value)
	if err != nil {
		return agro.INodeRef{}, err
	}
	v, ok := set.Chains[uint64(base.INode)]
	if !ok {
		return agro.INodeRef{}, err
	}
	return agro.NewINodeRef(base.Volume(), agro.INodeID(v)), nil
}

func (c *etcdCtx) SetChainINode(base agro.INodeRef, was agro.INodeRef, new agro.INodeRef) error {
	promOps.WithLabelValues("set-chain-inode").Inc()
	pageID := uint64ToHex(uint64(base.INode / chainPageSize))
	volume := uint64ToHex(uint64(base.Volume()))
	_, err := c.atomicModifyKey(mkKey("volumemeta", "chain", volume, pageID), func(b []byte) ([]byte, interface{}, error) {
		set := &models.FileChainSet{}
		if len(b) == 0 {
			set.Chains = make(map[uint64]uint64)
		} else {
			err := set.Unmarshal(b)
			if err != nil {
				return nil, nil, err
			}
		}
		v, ok := set.Chains[uint64(base.INode)]
		if !ok {
			v = 0
		}
		if v != uint64(was.INode) {
			return nil, nil, agro.ErrCompareFailed
		}
		if new.INode != 0 {
			set.Chains[uint64(base.INode)] = uint64(new.INode)
		} else {
			delete(set.Chains, uint64(base.INode))
		}
		b, err := set.Marshal()
		return b, was.INode, err
	})
	return err
}

func (c *etcdCtx) GetRing() (agro.Ring, error) {
	promOps.WithLabelValues("get-ring").Inc()
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

func (c *etcdCtx) GetVolumeLiveness(volumeID agro.VolumeID) (*roaring.Bitmap, []*roaring.Bitmap, error) {
	promOps.WithLabelValues("get-volume-liveness").Inc()
	volume := uint64ToHex(uint64(volumeID))
	tx := tx().Do(
		getKey(mkKey("volumemeta", "deadmap", volume)),
		getPrefix(mkKey("volumemeta", "open", volume)),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if err != nil {
		return nil, nil, err
	}
	deadmap := bytesToRoaring(resp.Responses[0].GetResponseRange().Kvs[0].Value)
	var l []*roaring.Bitmap
	for _, x := range resp.Responses[1].GetResponseRange().Kvs {
		l = append(l, bytesToRoaring(x.Value))
	}
	return deadmap, l, nil
}

func (c *etcdCtx) ClaimVolumeINodes(lease int64, volumeID agro.VolumeID, inodes *roaring.Bitmap) error {
	if lease == 0 {
		return errors.New("no lease")
	}
	promOps.WithLabelValues("claim-volume-inodes").Inc()
	volume := uint64ToHex(uint64(volumeID))
	key := mkKey("volumemeta", "open", volume, c.UUID())
	if inodes == nil {
		_, err := c.etcd.kv.DeleteRange(c.getContext(), deleteKey(key))
		return err
	}
	data := roaringToBytes(inodes)
	_, err := c.etcd.kv.Put(c.getContext(),
		setLeasedKey(lease, key, data),
	)
	return err
}

func (c *etcdCtx) ModifyDeadMap(volumeID agro.VolumeID, live *roaring.Bitmap, dead *roaring.Bitmap) error {
	promOps.WithLabelValues("modify-deadmap").Inc()
	if clog.LevelAt(capnslog.DEBUG) {
		newdead := roaring.AndNot(dead, live)
		clog.Tracef("killing %s", newdead.String())
		revive := roaring.AndNot(live, dead)
		clog.Tracef("reviving %s", revive.String())
	}
	volume := uint64ToHex(uint64(volumeID))
	_, err := c.atomicModifyKey(mkKey("volumemeta", "deadmap", volume), func(b []byte) ([]byte, interface{}, error) {
		bm := bytesToRoaring(b)
		bm.Or(dead)
		bm.AndNot(live)
		return roaringToBytes(bm), nil, nil
	})
	return err
}

func (c *etcdCtx) SetRing(ring agro.Ring) error {
	b, err := ring.Marshal()
	if err != nil {
		return err
	}
	key := mkKey("meta", "the-one-ring")
	_, err = c.etcd.kv.Put(c.getContext(),
		setKey(key, b))
	if err != nil {
		return err
	}
	return nil

}

func (c *etcdCtx) DumpMetadata(w io.Writer) error {
	io.WriteString(w, "## Volumes\n")
	resp, err := c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumeid")))
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		v := &models.Volume{}
		v.Unmarshal(x.Value)
		io.WriteString(w, v.String())
		io.WriteString(w, "\n")
	}
	io.WriteString(w, "## INodes\n")
	resp, err = c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumemeta", "inode")))
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		v := bytesToUint64(x.Value)
		io.WriteString(w, uint64ToHex(v))
		io.WriteString(w, "\n")
	}
	io.WriteString(w, "## BlockLocks\n")
	resp, err = c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumemeta", "blocklock")))
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		io.WriteString(w, string(x.Value))
		io.WriteString(w, "\n")
	}

	io.WriteString(w, "## Deadmaps\n")
	resp, err = c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumemeta", "deadmap")))
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		bm := bytesToRoaring(x.Value)
		io.WriteString(w, bm.String())
		io.WriteString(w, "\n")
	}
	io.WriteString(w, "## Open\n")
	resp, err = c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumemeta", "open")))
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		bm := bytesToRoaring(x.Value)
		io.WriteString(w, bm.String())
		io.WriteString(w, "\n")
	}
	io.WriteString(w, "## Dirs\n")
	resp, err = c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("dirs")))
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		dir := &models.Directory{}
		dir.Unmarshal(x.Value)
		io.WriteString(w, dir.String())
		io.WriteString(w, "\n")
	}
	io.WriteString(w, "## Chains\n")
	resp, err = c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumemeta", "chain")))
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		chains := &models.FileChainSet{}
		chains.Unmarshal(x.Value)
		io.WriteString(w, chains.String())
		io.WriteString(w, "\n")
	}
	return nil
}

func (c *etcdCtx) GetINodeChains(vid agro.VolumeID) ([]*models.FileChainSet, error) {
	volume := uint64ToHex(uint64(vid))
	resp, err := c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumemeta", "chain", volume)))
	if err != nil {
		return nil, err
	}
	var out []*models.FileChainSet
	for _, x := range resp.Kvs {
		chains := &models.FileChainSet{}
		chains.Unmarshal(x.Value)
		out = append(out, chains)
	}
	return out, nil
}
