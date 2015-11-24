package agro

import (
	"bytes"
	"encoding/binary"
	"fmt"

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

func (i INodeRef) String() string {
	return fmt.Sprintf("vol: %d, inode: %d", i.Volume, i.INode)
}

// BlockID is the identifier for a unique block in the filesystem.
type BlockID struct {
	INodeRef
	Index IndexID
}

const BlockIDByteSize = 8 * 3

func (b BlockID) ToBytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, BlockIDByteSize))
	binary.Write(buf, binary.LittleEndian, b)
	out := buf.Bytes()
	if len(out) != BlockIDByteSize {
		panic("breaking contract -- must make size appropriate")
	}
	return out
}

func BlockIDFromBytes(b []byte) BlockID {
	buf := bytes.NewBuffer(b)
	out := BlockID{}
	binary.Read(buf, binary.LittleEndian, &out)
	return out
}

func (b BlockID) String() string {
	i := b.INodeRef
	return fmt.Sprintf("vol: %d, inode: %d, block: %d", i.Volume, i.INode, b.Index)
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
