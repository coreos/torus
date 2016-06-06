package torus

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/torus/models"
	"golang.org/x/net/context"
)

// Blockset is the interface representing the standardized methods to interact
// with a set of blocks.
type Blockset interface {
	// Length returns the number of blocks in the Blockset.
	Length() int
	// Kind returns the kind of the Blockset.
	Kind() uint32
	// GetBlock returns the ith block in the Blockset.
	GetBlock(ctx context.Context, i int) ([]byte, error)
	// PutBlock puts a block with data `b` into the Blockset as its ith block.
	// The block belongs to the given inode.
	PutBlock(ctx context.Context, inode INodeRef, i int, b []byte) error
	// GetLiveInodes returns the current INode representation of the Blockset.
	// The returned INode might not be synced.
	GetLiveINodes() *roaring.Bitmap
	// GetAllBlockRefs returns the BlockRef of the blocks in the Blockset.
	// The ith BlockRef in the returned slice is the Ref of the ith Block in the
	// Blockset.
	GetAllBlockRefs() []BlockRef

	// Marshal returns the bytes representation of the Blockset.
	Marshal() ([]byte, error)
	// Unmarshal parses the bytes representation of the Blockset and stores the result
	// in the Blockset.
	Unmarshal(data []byte) error
	// GetSubBlockset gets the sub-Blockset of the Blockset if exists.
	// If there is no sub-Blockset, nil will be returned.
	GetSubBlockset() Blockset
	// Truncate changes the length of the Blockset and the block. If the Blockset has less
	// blocks than the required size, truncate adds zero blocks. If the block has less bytes
	// than required size, truncate add bytes into block.
	Truncate(lastIndex int, blocksize uint64) error
	// Trim zeros the blocks in range [from, to).
	Trim(from, to int) error
	// String implements the fmt.Stringer interface.
	String() string
}

type BlockLayerKind int

type BlockLayer struct {
	Kind    BlockLayerKind
	Options string
}

type BlockLayerSpec []BlockLayer

func MarshalBlocksetToProto(bs Blockset) ([]*models.BlockLayer, error) {
	var out []*models.BlockLayer
	var layer Blockset
	for layer = bs; layer != nil; layer = layer.GetSubBlockset() {
		m, err := layer.Marshal()
		if err != nil {
			return nil, err
		}
		out = append(out, &models.BlockLayer{
			Type:    layer.Kind(),
			Content: m,
		})
	}
	return out, nil
}
