package block

import (
	"errors"
	"path"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

type etcd struct {
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

func (c *etcdCtx) createBlockVol(volume *models.Volume) error {
	new, err := c.atomicModifyKey(mkKey("meta", "volumeminter"), bytesAddOne)
	volume.Id = new.(uint64)
	if err != nil {
		return err
	}
	vbytes, err := volume.Marshal()
	if err != nil {
		return err
	}
	inodeBytes := agro.NewINodeRef(agro.VolumeID(volume.Id), 1).ToBytes()
	do := tx().If(
		keyNotExists(mkKey("volumes", volume.Name)),
	).Then(
		setKey(mkKey("volumes", volume.Name), uint64ToBytes(volume.Id)),
		setKey(mkKey("volumeid", uint64ToHex(volume.Id)), vbytes),
		setKey(mkKey("volumemeta", "inode", uint64ToHex(volume.Id)), uint64ToBytes(1)),
		setKey(mkKey("volumemeta", "blockinode", uint64ToHex(volume.Id)), inodeBytes),
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

func (c *etcdCtx) LockBlockVolume(lease int64, vid agro.VolumeID) error {
	if lease == 0 {
		return agro.ErrInvalid
	}
	tx := tx().If(
		keyNotExists(mkKey("volumemeta", "blocklock", uint64ToHex(uint64(vid)))),
	).Then(
		setLeasedKey(lease, mkKey("volumemeta", "blocklock", uint64ToHex(uint64(vid))), []byte(c.UUID())),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrLocked
	}
	return nil
}

func (c *etcdCtx) GetBlockVolumeINode(vid agro.VolumeID) (agro.INodeRef, error) {
	resp, err := c.etcd.kv.Range(c.getContext(), getKey(mkKey("volumemeta", "blockinode", uint64ToHex(uint64(vid)))))
	if err != nil {
		return agro.NewINodeRef(0, 0), err
	}
	if len(resp.Kvs) != 1 {
		return agro.NewINodeRef(0, 0), errors.New("unexpected metadata for volume")
	}
	return agro.INodeRefFromBytes(resp.Kvs[0].Value), nil
}

func (c *etcdCtx) SyncBlockVolume(inode agro.INodeRef) error {
	vid := uint64(inode.Volume())
	inodeBytes := inode.ToBytes()
	tx := tx().If(
		keyExists(mkKey("volumemeta", "blocklock", uint64ToHex(vid))),
		keyEquals(mkKey("volumemeta", "blocklock", uint64ToHex(vid)), []byte(c.UUID())),
	).Then(
		setKey(mkKey("volumemeta", "blockinode", uint64ToHex(vid)), inodeBytes),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrLocked
	}
	return nil
}

func (c *etcdCtx) UnlockBlockVolume(v agro.VolumeID) error {
	vid := uint64(v)
	tx := tx().If(
		keyExists(mkKey("volumemeta", "blocklock", uint64ToHex(vid))),
		keyEquals(mkKey("volumemeta", "blocklock", uint64ToHex(vid)), []byte(c.UUID())),
	).Then(
		deleteKey(mkKey("volumemeta", "blocklock", uint64ToHex(vid))),
	).Tx()
	resp, err := c.etcd.kv.Txn(c.getContext(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrLocked
	}
	return nil
}
