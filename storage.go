package torus

import (
	"encoding/binary"
	"errors"
	"fmt"

	"golang.org/x/net/context"

	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/torus/models"
	"github.com/lpabon/godbc"
)

var BlockLog = capnslog.NewPackageLogger("github.com/coreos/torus", "blocklog")

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
	TypeBlock BlockType = iota
	TypeINode
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

// INodeRef is a reference to a unique INode in the cluster.
type INodeRef struct {
	volume VolumeID
	INode  INodeID
}

func (i INodeRef) Equals(x INodeRef) bool {
	return (i.volume == x.volume) && (i.INode == x.INode)
}

func (i INodeRef) Volume() VolumeID {
	return i.volume & VolumeMax
}

func (v VolumeID) ToBytes() []byte {
	buf := make([]byte, 8)
	order := binary.LittleEndian
	order.PutUint64(buf, uint64(v))
	return buf[:VolumeIDByteSize]
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
	godbc.Require(vol < VolumeMax, vol)
	return INodeRef{
		volume: vol & VolumeMax,
		INode:  i,
	}
}

const VolumeIDByteSize = 5
const INodeRefByteSize = 8 * 2

func (i INodeRef) ToBytes() []byte {
	buf := make([]byte, INodeRefByteSize)
	order := binary.LittleEndian
	order.PutUint64(buf[0:8], uint64(i.volume))
	order.PutUint64(buf[8:16], uint64(i.INode))
	return buf

}

func INodeRefFromBytes(b []byte) INodeRef {
	order := binary.LittleEndian
	return INodeRef{
		volume: VolumeID(order.Uint64(b[0:8])),
		INode:  INodeID(order.Uint64(b[8:16])),
	}
}

// BlockRef is the identifier for a unique block in the cluster.
type BlockRef struct {
	INodeRef
	Index IndexID
}

const BlockRefByteSize = 8 * 3

func (b BlockRef) ToBytes() []byte {
	buf := make([]byte, BlockRefByteSize)
	b.ToBytesBuf(buf)
	return buf
}

func (b BlockRef) ToBytesBuf(buf []byte) {
	order := binary.LittleEndian
	order.PutUint64(buf[0:8], uint64(b.volume))
	order.PutUint64(buf[8:16], uint64(b.INode))
	order.PutUint64(buf[16:24], uint64(b.Index))
}

func BlockRefFromBytes(b []byte) BlockRef {
	order := binary.LittleEndian
	ref := BlockRef{
		INodeRef: INodeRef{
			volume: VolumeID(order.Uint64(b[0:8])),
			INode:  INodeID(order.Uint64(b[8:16])),
		},
		Index: IndexID(order.Uint64(b[16:24])),
	}
	return ref
}

func BlockRefFromUint64s(volume, inode, index uint64) BlockRef {
	ref := BlockRef{
		INodeRef: INodeRef{
			volume: VolumeID(volume),
			INode:  INodeID(inode),
		},
		Index: IndexID(index),
	}
	return ref
}

func (b BlockRef) String() string {
	return fmt.Sprintf("br %x : %x : %x", b.volume, b.INode, b.Index)
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

func (b BlockRef) IsZero() bool {
	return b.Volume() == 0 && b.INode == 0 && b.Index == 0
}

func ZeroBlock() BlockRef {
	return BlockRef{}
}

func ZeroINode() INodeRef {
	return INodeRef{}
}

type ReadLevel int
type WriteLevel int

const (
	WriteAll WriteLevel = iota
	WriteOne
	WriteLocal
)

func ParseWriteLevel(s string) (wl WriteLevel, err error) {
	switch s {
	case "all":
		wl = WriteAll
	case "one":
		wl = WriteOne
	case "local":
		wl = WriteLocal
	default:
		err = errors.New("invalid writelevel; use one of 'one', 'all', or 'local'")
	}
	return
}

const (
	ReadBlock ReadLevel = iota
	ReadSequential
	ReadSpread
)

func ParseReadLevel(s string) (rl ReadLevel, err error) {
	switch s {
	case "spread":
		rl = ReadSpread
	case "seq":
		rl = ReadSequential
	case "block":
		rl = ReadBlock
	default:
		err = errors.New("invalid readlevel; use one of 'spread', 'seq', or 'block'")
	}
	return
}

// BlockStore is the interface representing the standardized methods to
// interact with something storing blocks.
type BlockStore interface {
	Store
	HasBlock(ctx context.Context, b BlockRef) (bool, error)
	GetBlock(ctx context.Context, b BlockRef) ([]byte, error)
	WriteBlock(ctx context.Context, b BlockRef, data []byte) error
	WriteBuf(ctx context.Context, b BlockRef) ([]byte, error)
	DeleteBlock(ctx context.Context, b BlockRef) error
	NumBlocks() uint64
	UsedBlocks() uint64
	BlockIterator() BlockIterator
	BlockSize() uint64
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
		panic("torus: attempted to register BlockStore " + name + " twice")
	}

	blockStores[name] = newFunc
}

func CreateBlockStore(kind string, name string, cfg Config, gmd GlobalMetadata) (BlockStore, error) {
	clog.Infof("creating blockstore: %s", kind)
	return blockStores[kind](name, cfg, gmd)
}
