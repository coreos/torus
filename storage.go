package agro

import (
	"encoding/binary"

	"github.com/barakmich/agro/models"
)

type (
	// VolumeID represents a unique identifier for a Volume.
	VolumeID uint64

	// IndexID represents a unique identifier for an Index.
	IndexID uint64

	// INodeID represents a unique identifier for an INode.
	INodeID uint64
)

// Store is the interface that represents methods that should be common across
// all types of storage providers.
type Store interface {
	Flush() error
	Close() error
}

// INodeRef is a reference to a unique INode in the filesystem.
type INodeRef struct {
	Volume VolumeID
	INode  INodeID
}

// BlockID is the identifier for a unique block in the filesystem.
type BlockID struct {
	INodeRef
	Index IndexID
}

const BlockIDByteSize = 8 * 3

func (b BlockID) ToBytes() []byte {
	out := make([]byte, BlockIDByteSize)
	n := binary.PutUvarint(out, uint64(b.INodeRef.Volume))
	out = out[n:]
	n = binary.PutUvarint(out, uint64(b.INodeRef.INode))
	out = out[n:]
	n = binary.PutUvarint(out, uint64(b.Index))
	return out
}

func BlockIDFromBytes(b []byte) BlockID {
	var n int
	v, n := binary.Uvarint(b)
	b = b[n:]
	o, n := binary.Uvarint(b)
	b = b[n:]
	i, n := binary.Uvarint(b)
	return BlockID{
		INodeRef: INodeRef{
			Volume: VolumeID(v),
			INode:  INodeID(o),
		},
		Index: IndexID(i),
	}
}

// BlockStore is the interface representing the standardized methods to
// interact with something storing blocks.
type BlockStore interface {
	Store
	GetBlock(b BlockID) ([]byte, error)
	WriteBlock(b BlockID, data []byte) error
	DeleteBlock(b BlockID) error
}

// INodeStore is the interface representing the standardized methods to
// interact with something storing INodes.
type INodeStore interface {
	Store
	GetINode(i INodeRef) (*models.INode, error)
	WriteINode(i INodeRef, inode *models.INode) error
	DeleteINode(i INodeRef) error
}
