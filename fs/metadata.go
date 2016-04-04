package fs

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/tgruben/roaring"
)

type FSVolume interface {
	agro.MetadataService

	Mkdir(path Path, dir *models.Metadata) error
	ChangeDirMetadata(path Path, dir *models.Metadata) error
	Getdir(path Path) (*models.Directory, []Path, error)
	Rmdir(path Path) error
	SetFileEntry(path Path, ent *models.FileEntry) error

	GetINodeChains(vid agro.VolumeID) ([]*models.FileChainSet, error)
	GetChainINode(base agro.INodeRef) (agro.INodeRef, error)
	SetChainINode(base agro.INodeRef, was agro.INodeRef, new agro.INodeRef) error

	ClaimVolumeINodes(lease int64, vol agro.VolumeID, inodes *roaring.Bitmap) error

	ModifyDeadMap(vol agro.VolumeID, live *roaring.Bitmap, dead *roaring.Bitmap) error
	GetVolumeLiveness(vol agro.VolumeID) (*roaring.Bitmap, []*roaring.Bitmap, error)
}

func OpenFSVolume(mds agro.MetadataService, name string) FSVolume {
	panic("unimplemented -- only works with etcd metadata")
}

func CreateFSVolume(mds agro.MetadataService, name string) error {
	panic("unimplemented -- only works with etcd metadata")
}
