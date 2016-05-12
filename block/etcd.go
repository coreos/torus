package block

import (
	"encoding/json"
	"errors"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"

	"github.com/coreos/agro"
	"github.com/coreos/agro/metadata/etcd"
	"github.com/coreos/agro/models"
)

type blockEtcd struct {
	*etcd.Etcd
	name string
	vid  agro.VolumeID
}

func (b *blockEtcd) CreateBlockVolume(volume *models.Volume) error {
	new, err := b.AtomicModifyKey([]byte(etcd.MkKey("meta", "volumeminter")), etcd.BytesAddOne)
	volume.Id = new.(uint64)
	if err != nil {
		return err
	}
	vbytes, err := volume.Marshal()
	if err != nil {
		return err
	}
	inodeBytes := agro.NewINodeRef(agro.VolumeID(volume.Id), 1).ToBytes()

	do := b.Etcd.Client.Txn(b.getContext()).If(
		etcdv3.Compare(etcdv3.Version(etcd.MkKey("volumes", volume.Name)), "=", 0),
	).Then(
		etcdv3.OpPut(etcd.MkKey("volumes", volume.Name), string(etcd.Uint64ToBytes(volume.Id))),
		etcdv3.OpPut(etcd.MkKey("volumeid", etcd.Uint64ToHex(volume.Id)), string(vbytes)),
		etcdv3.OpPut(etcd.MkKey("volumemeta", etcd.Uint64ToHex(volume.Id), "inode"), string(etcd.Uint64ToBytes(1))),
		etcdv3.OpPut(etcd.MkKey("volumemeta", etcd.Uint64ToHex(volume.Id), "blockinode"), string(inodeBytes)),
	)
	resp, err := do.Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrExists
	}
	return nil
}

func (b *blockEtcd) DeleteVolume() error {
	vid := uint64(b.vid)
	tx := b.Etcd.Client.Txn(b.getContext()).If(
		etcdv3.Compare(etcdv3.Version(etcd.MkKey("volumemeta", etcd.Uint64ToHex(vid), "blocklock")), "=", 0),
	).Then(
		etcdv3.OpDelete(etcd.MkKey("volumes", b.name)),
		etcdv3.OpDelete(etcd.MkKey("volumeid", etcd.Uint64ToHex(vid))),
		etcdv3.OpDelete(etcd.MkKey("volumemeta", etcd.Uint64ToHex(vid)), etcdv3.WithPrefix()),
	)
	resp, err := tx.Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrLocked
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
	k := etcd.MkKey("volumemeta", etcd.Uint64ToHex(uint64(b.vid)), "blocklock")
	tx := b.Etcd.Client.Txn(b.getContext()).If(
		etcdv3.Compare(etcdv3.Version(k), "=", 0),
	).Then(
		etcdv3.OpPut(k, b.Etcd.UUID(), etcdv3.WithLease(etcdv3.LeaseID(lease))),
	)
	resp, err := tx.Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrLocked
	}
	return nil
}

func (b *blockEtcd) GetINode() (agro.INodeRef, error) {
	resp, err := b.Etcd.Client.Get(b.getContext(), etcd.MkKey("volumemeta", etcd.Uint64ToHex(uint64(b.vid)), "blockinode"))
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
	inodeBytes := string(inode.ToBytes())
	k := etcd.MkKey("volumemeta", etcd.Uint64ToHex(vid), "blocklock")
	tx := b.Etcd.Client.Txn(b.getContext()).If(
		etcdv3.Compare(etcdv3.Version(k), ">", 0),
		etcdv3.Compare(etcdv3.Value(k), "=", b.Etcd.UUID()),
	).Then(
		etcdv3.OpPut(etcd.MkKey("volumemeta", etcd.Uint64ToHex(vid), "blockinode"), inodeBytes),
	)
	resp, err := tx.Commit()
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
	k := etcd.MkKey("volumemeta", etcd.Uint64ToHex(vid), "blocklock")
	tx := b.Etcd.Client.Txn(b.getContext()).If(
		etcdv3.Compare(etcdv3.Version(k), ">", 0),
		etcdv3.Compare(etcdv3.Value(k), "=", b.Etcd.UUID()),
	).Then(
		etcdv3.OpDelete(etcd.MkKey("volumemeta", etcd.Uint64ToHex(vid), "blocklock")),
	)
	resp, err := tx.Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrLocked
	}
	return nil
}

func (b *blockEtcd) SaveSnapshot(name string) error {
	vid := uint64(b.vid)
	for {
		sshotKey := etcd.MkKey("volumemeta", etcd.Uint64ToHex(vid), "snapshots", name)
		inoKey := etcd.MkKey("volumemeta", etcd.Uint64ToHex(vid), "blockinode")
		tx := b.Etcd.Client.Txn(b.getContext()).If(
			etcdv3.Compare(etcdv3.Version(sshotKey), "=", 0),
		).Then(
			etcdv3.OpGet(inoKey),
		)
		resp, err := tx.Commit()
		if err != nil {
			return err
		}
		if !resp.Succeeded {
			return agro.ErrExists
		}
		v := resp.Responses[0].GetResponseRange().Kvs[0]
		inode := Snapshot{
			Name:     name,
			INodeRef: v.Value,
		}
		bytes, err := json.Marshal(inode)
		if err != nil {
			return err
		}
		tx = b.Etcd.Client.Txn(b.getContext()).If(
			etcdv3.Compare(etcdv3.Version(inoKey), "=", v.Version),
		).Then(
			etcdv3.OpPut(sshotKey, string(bytes)),
		)
		resp, err = tx.Commit()
		if err != nil {
			return err
		}
		if !resp.Succeeded {
			continue
		}
		return nil
	}

}

func (b *blockEtcd) GetSnapshots() ([]Snapshot, error) {
	resp, err := b.Etcd.Client.Get(b.getContext(),
		etcd.MkKey("volumemeta", etcd.Uint64ToHex(uint64(b.vid)), "snapshots"),
		etcdv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]Snapshot, len(resp.Kvs))
	for i, r := range resp.Kvs {
		err := json.Unmarshal(r.Value, &out[i])
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (b *blockEtcd) DeleteSnapshot(name string) error {
	vid := uint64(b.vid)
	k := etcd.MkKey("volumemeta", etcd.Uint64ToHex(vid), "snapshots", name)
	tx := b.Etcd.Client.Txn(b.getContext()).If(
		etcdv3.Compare(etcdv3.Version(k), ">", 0),
	).Then(
		etcdv3.OpDelete(k),
	)
	resp, err := tx.Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return agro.ErrLocked
	}
	return nil
}

func createBlockEtcdMetadata(mds agro.MetadataService, name string, vid agro.VolumeID) (blockMetadata, error) {
	if e, ok := mds.(*etcd.Etcd); ok {
		return &blockEtcd{
			Etcd: e,
			name: name,
			vid:  vid,
		}, nil
	}
	panic("how are we creating an etcd metadata that doesn't implement it but reports as being etcd")
}
