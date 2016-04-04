package block

import (
	"github.com/coreos/agro"
	"github.com/coreos/pkg/capnslog"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "block")

type INodeVolume interface {
	agro.MetadataService

	CommitINodeIndex() (INodeID, error)
	GetINodeIndex() (INodeID, error)
	GetINodeIndexes() (map[string]INodeID, error)
}

type BlockVolume interface {
	INodeVolume

	Lock(lease int64) error
	GetBlockVolumeINode() (INodeRef, error)
	SyncBlockVolume(INodeRef) error
	Unlock() error
}

func OpenBlockVolume(mds agro.MetadataService, name string) BlockVolume {
	panic("unimplemented -- only works with etcd metadata")
}
