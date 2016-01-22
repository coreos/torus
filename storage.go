package agro

import (
	"encoding/binary"
	"fmt"

	"golang.org/x/net/context"

	"github.com/coreos/agro/models"
)

type (
	// VolumeID represents a unique identifier for a Volume.
	VolumeID uint64

	// IndexID represents a unique identifier for an Index.
	IndexID uint64

	// INodeID represents a unique identifier for an INode.
	INodeID uint64

	BlockType uint16
)

const (
	Block BlockType = iota
	INode
)

const (
	VolumeMax = 0x000000FFFFFFFFFF
)

// Store is the interface that represents methods that should be common across
// all types of storage providers.
type Store interface {
	Kind() string
	Flush() error
	Close() error
}

// INodeRef is a reference to a unique INode in the filesystem.
type INodeRef struct {
	volume VolumeID
	INode  INodeID
}

func (i INodeRef) Volume() VolumeID {
	return i.volume & VolumeMax
}

func (i INodeRef) String() string {
	return fmt.Sprintf("vol: %d, inode: %d", i.Volume(), i.INode)
}

func (i INodeRef) ToProto() *models.INodeRef {
	return &models.INodeRef{
		Volume: uint64(i.volume),
		INode:  uint64(i.INode),
	}
}

func NewINodeRef(vol VolumeID, i INodeID) INodeRef {
	return INodeRef{
		volume: vol & VolumeMax,
		INode:  i,
	}
}

const INodeRefByteSize = 8 * 2

func (i INodeRef) ToBytes() []byte {
	buf := make([]byte, INodeRefByteSize)
	order := binary.LittleEndian
	order.PutUint64(buf[0:8], uint64(i.volume))
	order.PutUint64(buf[8:16], uint64(i.INode))
	return buf
}

// BlockRef is the identifier for a unique block in the filesystem.
type BlockRef struct {
	INodeRef
	Index IndexID
}

const BlockRefByteSize = 8 * 3

func (b BlockRef) ToBytes() []byte {
	buf := make([]byte, BlockRefByteSize)
	order := binary.LittleEndian
	order.PutUint64(buf[0:8], uint64(b.volume))
	order.PutUint64(buf[8:16], uint64(b.INode))
	order.PutUint64(buf[16:24], uint64(b.Index))
	return buf
}

func BlockRefFromBytes(b []byte) BlockRef {
	order := binary.LittleEndian
	ref := BlockRef{
		INodeRef: NewINodeRef(
			VolumeID(order.Uint64(b[0:8])),
			INodeID(order.Uint64(b[8:16])),
		),
		Index: IndexID(order.Uint64(b[16:24])),
	}
	return ref
}

func (b BlockRef) String() string {
	return fmt.Sprintf("br %d : %d : %d", b.volume, b.INode, b.Index)
}

func (b BlockRef) ToProto() *models.BlockRef {
	return &models.BlockRef{
		Volume: uint64(b.volume),
		INode:  uint64(b.INode),
		Block:  uint64(b.Index),
	}
}

func BlockFromProto(p *models.BlockRef) BlockRef {
	return BlockRef{
		INodeRef: INodeRef{
			volume: VolumeID(p.Volume),
			INode:  INodeID(p.INode),
		},
		Index: IndexID(p.Block),
	}
}

func INodeFromProto(p *models.INodeRef) INodeRef {
	return INodeRef{
		volume: VolumeID(p.Volume),
		INode:  INodeID(p.INode),
	}
}

func (b BlockRef) HasINode(i INodeRef, t BlockType) bool {
	return b.INode == i.INode && b.Volume() == i.Volume() && b.BlockType() == t
}

func (b BlockRef) BlockType() BlockType {
	return BlockType((b.volume &^ VolumeMax) >> 40)
}

func (b *BlockRef) SetBlockType(t BlockType) {
	b.volume = VolumeID(uint64(b.volume) | ((uint64(t) & 0xFFFFFF) << 40))
}

// BlockStore is the interface representing the standardized methods to
// interact with something storing blocks.
type BlockStore interface {
	Store
	GetBlock(ctx context.Context, b BlockRef) ([]byte, error)
	WriteBlock(ctx context.Context, b BlockRef, data []byte) error
	DeleteBlock(ctx context.Context, b BlockRef) error
	DeleteINodeBlocks(ctx context.Context, b INodeRef) error
	NumBlocks() uint64
	UsedBlocks() uint64
	BlockIterator() BlockIterator
	ReplaceBlockStore(BlockStore) (BlockStore, error)
	// TODO(barakmich) FreeBlocks()
}

type BlockIterator interface {
	Err() error
	Next() bool
	BlockRef() BlockRef
	Close() error
}

type NewBlockStoreFunc func(string, Config, GlobalMetadata) (BlockStore, error)

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

func CreateBlockStore(kind string, name string, cfg Config, gmd GlobalMetadata) (BlockStore, error) {
	clog.Infof("creating blockstore: %s", kind)
	return blockStores[kind](name, cfg, gmd)
}

// INodeStore is the interface representing the standardized methods to
// interact with something storing INodes.
type INodeStore interface {
	Store
	GetINode(ctx context.Context, i INodeRef) (*models.INode, error)
	WriteINode(ctx context.Context, i INodeRef, inode *models.INode) error
	DeleteINode(ctx context.Context, i INodeRef) error
	INodeIterator() INodeIterator
	ReplaceINodeStore(INodeStore) (INodeStore, error)
}

type INodeIterator interface {
	Err() error
	Next() bool
	INodeRef() INodeRef
	Close() error
}

type NewINodeStoreFunc func(string, Config) (INodeStore, error)

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

func CreateINodeStore(kind string, name string, cfg Config) (INodeStore, error) {
	clog.Infof("creating inode store: %s", kind)
	return inodeStores[kind](name, cfg)
}
