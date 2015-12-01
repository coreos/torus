package agro

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"golang.org/x/net/context"

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

// BlockRef is the identifier for a unique block in the filesystem.
type BlockRef struct {
	INodeRef
	Index IndexID
}

const BlockRefByteSize = 8 * 3

func (b BlockRef) ToBytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, BlockRefByteSize))
	binary.Write(buf, binary.LittleEndian, b)
	out := buf.Bytes()
	if len(out) != BlockRefByteSize {
		panic("breaking contract -- must make size appropriate")
	}
	return out
}

func BlockRefFromBytes(b []byte) BlockRef {
	buf := bytes.NewBuffer(b)
	out := BlockRef{}
	binary.Read(buf, binary.LittleEndian, &out)
	return out
}

func (b BlockRef) String() string {
	i := b.INodeRef
	return fmt.Sprintf("vol: %d, inode: %d, block: %d", i.Volume, i.INode, b.Index)
}

// BlockStore is the interface representing the standardized methods to
// interact with something storing blocks.
type BlockStore interface {
	Store
	GetBlock(ctx context.Context, b BlockRef) ([]byte, error)
	WriteBlock(ctx context.Context, b BlockRef, data []byte) error
	DeleteBlock(ctx context.Context, b BlockRef) error
	NumBlocks() uint64
	// TODO(barakmich) FreeBlocks()
}

type NewBlockStoreFunc func(Config, GlobalMetadata) (BlockStore, error)

var blockStores map[string]NewBlockStoreFunc

func RegisterBlockStore(name string, newFunc NewBlockStoreFunc) {
	if blockStores == nil {
		blockStores = make(map[string]NewBlockStoreFunc)
	}

	if _, ok := blockStores[name]; ok {
		panic("agro: attempted to register BlockStore " + name + " twice")
	}

	blockStores[name] = newFunc
}

func CreateBlockStore(name string, cfg Config, gmd GlobalMetadata) (BlockStore, error) {
	clog.Infof("creating blockstore: %s", name)
	return blockStores[name](cfg, gmd)
}

// INodeStore is the interface representing the standardized methods to
// interact with something storing INodes.
type INodeStore interface {
	Store
	GetINode(ctx context.Context, i INodeRef) (*models.INode, error)
	WriteINode(ctx context.Context, i INodeRef, inode *models.INode) error
	DeleteINode(ctx context.Context, i INodeRef) error
}

type NewINodeStoreFunc func(Config) (INodeStore, error)

var inodeStores map[string]NewINodeStoreFunc

func RegisterINodeStore(name string, newFunc NewINodeStoreFunc) {
	if inodeStores == nil {
		inodeStores = make(map[string]NewINodeStoreFunc)
	}

	if _, ok := inodeStores[name]; ok {
		panic("agro: attempted to register INodeStore " + name + " twice")
	}

	inodeStores[name] = newFunc
}

func CreateINodeStore(name string, cfg Config) (INodeStore, error) {
	clog.Infof("creating inode store: %s", name)
	return inodeStores[name](cfg)
}
