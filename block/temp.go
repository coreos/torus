package block

import (
	"fmt"

	"github.com/coreos/agro"
	"github.com/coreos/agro/metadata/temp"
	"github.com/coreos/agro/models"
)

type blockTempMetadata struct {
	*temp.Client
	vid agro.VolumeID
}

type blockTempVolumeData struct {
	locked string
	id     agro.INodeRef
}

func (b *blockTempMetadata) CreateBlockVolume(volume *models.Volume) error {
	b.LockData()
	defer b.UnlockData()
	_, ok := b.GetData(fmt.Sprint(volume.Id))
	if ok {
		return agro.ErrExists
	}
	b.SetData(fmt.Sprint(volume.Id), &blockTempVolumeData{
		locked: "",
		id:     agro.NewINodeRef(agro.VolumeID(volume.Id), 1),
	})
	return nil
}

func (b *blockTempMetadata) Lock(lease int64) error {
	b.LockData()
	defer b.UnlockData()
	v, ok := b.GetData(fmt.Sprint(b.vid))
	if !ok {
		return agro.ErrNotExist
	}
	d := v.(*blockTempVolumeData)
	if d.locked != "" {
		return agro.ErrLocked
	}
	d.locked = b.UUID()
	return nil
}

func (b *blockTempMetadata) GetINode() (agro.INodeRef, error) {
	b.LockData()
	defer b.UnlockData()
	v, ok := b.GetData(fmt.Sprint(b.vid))
	if !ok {
		return agro.ZeroINode(), agro.ErrNotExist
	}
	d := v.(*blockTempVolumeData)
	return d.id, nil
}

func (b *blockTempMetadata) SyncINode(inode agro.INodeRef) error {
	b.LockData()
	defer b.UnlockData()
	v, ok := b.GetData(fmt.Sprint(b.vid))
	if !ok {
		return agro.ErrNotExist
	}
	d := v.(*blockTempVolumeData)
	if d.locked != b.UUID() {
		return agro.ErrLocked
	}
	d.id = inode
	return nil
}

func (b *blockTempMetadata) Unlock() error {
	b.LockData()
	defer b.UnlockData()
	v, ok := b.GetData(fmt.Sprint(b.vid))
	if !ok {
		return agro.ErrNotExist
	}
	d := v.(*blockTempVolumeData)
	if d.locked != b.UUID() {
		return agro.ErrLocked
	}
	d.locked = ""
	return nil
}

func createBlockTempMetadata(mds agro.MetadataService, vid agro.VolumeID) (blockMetadata, error) {
	if t, ok := mds.(*temp.Client); ok {
		return &blockTempMetadata{
			Client: t,
			vid:    vid,
		}, nil
	}
	panic("how are we creating a temp metadata that doesn't implement it but reports as being temp")
}
