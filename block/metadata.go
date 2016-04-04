package block

import (
	"github.com/coreos/agro"
	"github.com/coreos/pkg/capnslog"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "block")

type BlockVolume interface {
	agro.MetadataService

	Lock(lease int64) error
	GetBlockVolumeINode() (agro.INodeRef, error)
	SyncBlockVolume(agro.INodeRef) error
	Unlock() error
}

func OpenBlockVolume(mds agro.MetadataService, name string) BlockVolume {
	panic("unimplemented -- only works with etcd metadata")
}

func CreateBlockVolume(mds agro.MetadataService, name string) error {
	panic("unimplemented -- only works with etcd metadata")
}
