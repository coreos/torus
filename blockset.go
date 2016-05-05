package agro

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/agro/models"
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
	GetAllBlockRefs() []BlockRef

	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
	GetSubBlockset() Blockset
	Truncate(lastIndex int, blocksize uint64) error
	Trim(from, to int) error
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
