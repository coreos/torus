package block

import (
	"errors"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/coreos/pkg/capnslog"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "block")

type Snapshot struct {
	Name     string
	INodeRef []byte
}

type blockMetadata interface {
	agro.MetadataService

	Lock(lease int64) error
	Unlock() error

	GetINode() (agro.INodeRef, error)
	SyncINode(agro.INodeRef) error

	CreateBlockVolume(vol *models.Volume) error
	DeleteVolume() error

	SaveSnapshot(name string) error
	GetSnapshots() ([]Snapshot, error)
	DeleteSnapshot(name string) error
}

func createBlockMetadata(mds agro.MetadataService, name string, vid agro.VolumeID) (blockMetadata, error) {
	switch mds.Kind() {
	case agro.EtcdMetadata:
		return createBlockEtcdMetadata(mds, name, vid)
	case agro.TempMetadata:
		return createBlockTempMetadata(mds, name, vid)
	default:
		return nil, errors.New("unimplemented for this kind of metadata")
	}
}
