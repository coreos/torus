package block

import (
	"errors"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
	"github.com/coreos/agro/metadata/etcd"
	"github.com/coreos/agro/models"
)

type blockEtcd struct {
	*etcd.Etcd
	vid agro.VolumeID
}

func (b *blockEtcd) CreateBlockVolume(volume *models.Volume) error {
	new, err := b.AtomicModifyKey(etcd.MkKey("meta", "volumeminter"), etcd.BytesAddOne)
	volume.Id = new.(uint64)
	if err != nil {
		return err
	}
	vbytes, err := volume.Marshal()
	if err != nil {
		return err
	}
	inodeBytes := agro.NewINodeRef(agro.VolumeID(volume.Id), 1).ToBytes()
	do := etcd.Tx().If(
		etcd.KeyNotExists(etcd.MkKey("volumes", volume.Name)),
	).Then(
		etcd.SetKey(etcd.MkKey("volumes", volume.Name), etcd.Uint64ToBytes(volume.Id)),
		etcd.SetKey(etcd.MkKey("volumeid", etcd.Uint64ToHex(volume.Id)), vbytes),
		etcd.SetKey(etcd.MkKey("volumemeta", "inode", etcd.Uint64ToHex(volume.Id)), etcd.Uint64ToBytes(1)),
		etcd.SetKey(etcd.MkKey("volumemeta", "blockinode", etcd.Uint64ToHex(volume.Id)), inodeBytes),
	).Tx()
	resp, err := b.Etcd.KV.Txn(b.getContext(), do)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrExists
	}
	return nil
}

func (b *blockEtcd) getContext() context.Context {
	return context.TODO()
}

func (b *blockEtcd) Lock(lease int64) error {
	if lease == 0 {
		return agro.ErrInvalid
	}
	tx := etcd.Tx().If(
		etcd.KeyNotExists(etcd.MkKey("volumemeta", "blocklock", etcd.Uint64ToHex(uint64(b.vid)))),
	).Then(
		etcd.SetLeasedKey(lease, etcd.MkKey("volumemeta", "blocklock", etcd.Uint64ToHex(uint64(b.vid))), []byte(b.Etcd.UUID())),
	).Tx()
	resp, err := b.Etcd.KV.Txn(b.getContext(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrLocked
	}
	return nil
}

func (b *blockEtcd) GetINode() (agro.INodeRef, error) {
	resp, err := b.Etcd.KV.Range(b.getContext(), etcd.GetKey(etcd.MkKey("volumemeta", "blockinode", etcd.Uint64ToHex(uint64(b.vid)))))
	if err != nil {
		return agro.NewINodeRef(0, 0), err
	}
	if len(resp.Kvs) != 1 {
		return agro.NewINodeRef(0, 0), errors.New("unexpected metadata for volume")
	}
	return agro.INodeRefFromBytes(resp.Kvs[0].Value), nil
}

func (b *blockEtcd) SyncINode(inode agro.INodeRef) error {
	vid := uint64(inode.Volume())
	inodeBytes := inode.ToBytes()
	tx := etcd.Tx().If(
		etcd.KeyExists(etcd.MkKey("volumemeta", "blocklock", etcd.Uint64ToHex(vid))),
		etcd.KeyEquals(etcd.MkKey("volumemeta", "blocklock", etcd.Uint64ToHex(vid)), []byte(b.Etcd.UUID())),
	).Then(
		etcd.SetKey(etcd.MkKey("volumemeta", "blockinode", etcd.Uint64ToHex(vid)), inodeBytes),
	).Tx()
	resp, err := b.Etcd.KV.Txn(b.getContext(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrLocked
	}
	return nil
}

func (b *blockEtcd) Unlock() error {
	vid := uint64(b.vid)
	tx := etcd.Tx().If(
		etcd.KeyExists(etcd.MkKey("volumemeta", "blocklock", etcd.Uint64ToHex(vid))),
		etcd.KeyEquals(etcd.MkKey("volumemeta", "blocklock", etcd.Uint64ToHex(vid)), []byte(b.Etcd.UUID())),
	).Then(
		etcd.DeleteKey(etcd.MkKey("volumemeta", "blocklock", etcd.Uint64ToHex(vid))),
	).Tx()
	resp, err := b.Etcd.KV.Txn(b.getContext(), tx)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrLocked
	}
	return nil
}

func createBlockEtcdMetadata(mds agro.MetadataService, vid agro.VolumeID) (blockMetadata, error) {
	if e, ok := mds.(*etcd.Etcd); ok {
		return &blockEtcd{
			Etcd: e,
			vid:  vid,
		}, nil
	}
	panic("how are we creating an etcd metadata that doesn't implement it but reports as being etcd")
}
