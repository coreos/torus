package fs

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/tgruben/roaring"
)

type FSMetadataService interface {
	agro.MetadataService

	Mkdir(path Path, dir *models.Metadata) error
	ChangeDirMetadata(path Path, dir *models.Metadata) error
	Getdir(path Path) (*models.Directory, []Path, error)
	Rmdir(path Path) error
	SetFileEntry(path Path, ent *models.FileEntry) error

	GetINodeChains(vid VolumeID) ([]*models.FileChainSet, error)
	GetChainINode(base INodeRef) (INodeRef, error)
	SetChainINode(base INodeRef, was INodeRef, new INodeRef) error

	ClaimVolumeINodes(lease int64, vol VolumeID, inodes *roaring.Bitmap) error

	ModifyDeadMap(vol VolumeID, live *roaring.Bitmap, dead *roaring.Bitmap) error
	GetVolumeLiveness(vol VolumeID) (*roaring.Bitmap, []*roaring.Bitmap, error)
}
