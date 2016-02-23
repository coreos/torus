package gc

import (
	"sync"

	"github.com/RoaringBitmap/roaring"

	"github.com/coreos/agro"
)

type blocksByINode struct {
	mut     sync.RWMutex
	deadmap *roaring.Bitmap
	vol     agro.VolumeID
	mds     agro.MetadataService
}

func NewBlocksByINodeGC(mds agro.MetadataService) GC {
	return &blocksByINode{mds: mds}
}

func (b *blocksByINode) PrepVolume(vid agro.VolumeID) error {
	b.mut.Lock()
	defer b.mut.Unlock()
	b.vol = vid
	deadmap, held, err := b.mds.GetVolumeLiveness(vid)
	if err != nil {
		return err
	}
	for _, x := range held {
		deadmap.AndNot(x)
	}
	b.deadmap = deadmap
	return nil
}

func (b *blocksByINode) IsDead(ref agro.BlockRef) bool {
	b.mut.RLock()
	defer b.mut.RUnlock()
	if ref.Volume() != b.vol {
		clog.Error("checking dead ref we haven't prepared for")
		return false
	}

	if b.deadmap.Contains(uint32(ref.INode)) {
		return true
	}
	return false
}

func (b *blocksByINode) Clear() {}
