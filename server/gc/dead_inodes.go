package gc

import (
	"sync"

	"github.com/coreos/agro"
	"github.com/tgruben/roaring"
)

// TODO(barakmich): This should really be based on truly dead ones
// Since chains can go backwards, under heavy write contention we could,
// theoretically, lose an INode.
//
// There's a simple fix for this -- print a higher INode at file.Sync()
// time, if we appear to be going backwards. That way chains are
// strictly increasing and we're fine.

type deadINodes struct {
	mut  sync.RWMutex
	live *roaring.Bitmap
	vol  agro.VolumeID
	max  agro.INodeID
	mds  agro.MetadataService
}

func NewDeadINodeGC(mds agro.MetadataService) GC {
	return &deadINodes{mds: mds}
}

func (d *deadINodes) PrepVolume(vid agro.VolumeID) error {
	d.mut.Lock()
	defer d.mut.Unlock()
	d.vol = vid
	chains, err := d.mds.GetINodeChains(vid)
	if err != nil {
		return err
	}
	max := uint64(0)
	bm := roaring.NewBitmap()
	for _, c := range chains {
		for _, v := range c.Chains {
			if v > max {
				max = v
			}
			bm.Add(uint32(v))
		}
	}
	d.max = agro.INodeID(max)
	d.live = bm
	return nil
}

func (d *deadINodes) IsDead(ref agro.BlockRef) bool {
	if ref.BlockType() != agro.TypeINode {
		return false
	}
	if ref.INode >= d.max {
		return false
	}
	if d.live.Contains(uint32(ref.INode)) {
		return false
	}
	return true
}

func (d *deadINodes) Clear() {}
