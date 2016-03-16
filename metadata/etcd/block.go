package etcd

import (
	"errors"

	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/models"
)

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
	globals, err := c.GlobalMetadata()
	if err != nil {
		return err
	}
	bs, err := blockset.CreateBlocksetFromSpec(globals.DefaultBlockSpec, nil)
	if err != nil {
		return err
	}
	nBlocks := (volume.MaxBytes / globals.BlockSize)
	if volume.MaxBytes%globals.BlockSize != 0 {
		nBlocks++
	}
	err = bs.Truncate(int(nBlocks), globals.BlockSize)
	if err != nil {
		return err
	}
	inode := models.NewEmptyINode()
	inode.INode = 1
	inode.Volume = volume.Id
	inode.Filesize = volume.MaxBytes
	inode.Blocks, err = blockset.MarshalToProto(bs)
	if err != nil {
		return err
	}
	inodeBytes, err := inode.Marshal()
	if err != nil {
		return err
	}
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

func (c *etcdCtx) SyncBlockVolume(inode *agro.INodeRef) error {
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
