package agro

import "github.com/barakmich/agro/models"

type VolumeID uint64
type IndexID uint64
type INodeID uint64

type Store interface {
	Flush() error
	Close() error
}

type INodeRef struct {
	Volume VolumeID
	INode  INodeID
}

type BlockID struct {
	INodeRef
	Index IndexID
}

type BlockStore interface {
	Store
	GetBlock(b BlockID) ([]byte, error)
	WriteBlock(b BlockID, data []byte) error
	DeleteBlock(b BlockID) error
}

type INodeStore interface {
	Store
	GetINode(i INodeRef) (*models.INode, error)
	WriteINode(i INodeRef, inode *models.INode) error
	DeleteINode(i INodeRef) error
}
