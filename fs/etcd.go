package fs

import (
	"bytes"
	"errors"
	"os"
	"time"

	"github.com/coreos/agro/models"
	"github.com/coreos/pkg/capnslog"
	"github.com/tgruben/roaring"
)

const (
	chainPageSize = 1000
)

type etcd struct {
}

func (c *etcd) createFSVol(volume *models.Volume) error {
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
