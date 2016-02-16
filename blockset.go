package agro

import (
	"github.com/RoaringBitmap/roaring"
	"golang.org/x/net/context"
)

// Blockset is the interface representing the standardized methods to interact
// with a set of blocks.
type Blockset interface {
	Length() int
	Kind() uint32
	GetBlock(ctx context.Context, i int) ([]byte, error)
	PutBlock(ctx context.Context, inode INodeRef, i int, b []byte) error
	GetLiveINodes() *roaring.Bitmap

	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
	GetSubBlockset() Blockset
	Truncate(lastIndex int) error
}

type BlockLayerKind int

type BlockLayer struct {
	Kind    BlockLayerKind
	Options string
}

type BlockLayerSpec []BlockLayer
