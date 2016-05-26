package block

import (
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/torus"
	"github.com/coreos/torus/blockset"
	"github.com/coreos/torus/gc"
	"github.com/coreos/torus/models"
)

func init() {
	gc.RegisterGC("blockvol", NewBlockVolGC)
}

type blockvolGC struct {
	srv        *torus.Server
	inodes     gc.INodeFetcher
	set        map[torus.BlockRef]bool
	highwaters map[torus.VolumeID]torus.INodeID
	curINodes  []torus.INodeRef
}

func NewBlockVolGC(srv *torus.Server, inodes gc.INodeFetcher) (gc.GC, error) {
	b := &blockvolGC{
		srv:    srv,
		inodes: inodes,
	}
	b.Clear()
	return b, nil
}

func (b *blockvolGC) getContext() context.Context {
	ctx, _ := context.WithTimeout(context.TODO(), 2*time.Second)
	return b.srv.ExtendContext(ctx)
}

func (b *blockvolGC) PrepVolume(vol *models.Volume) error {
	if vol.Type != VolumeType {
		return nil
	}
	mds, err := createBlockMetadata(b.srv.MDS, vol.Name, torus.VolumeID(vol.Id))
	if err != nil {
		return err
	}
	curRef, err := mds.GetINode()
	if err != nil {
		return err
	}
	b.highwaters[curRef.Volume()] = 0
	if curRef.INode <= 1 {
		return nil
	}

	snaps, err := mds.GetSnapshots()
	if err != nil {
		return err
	}

	curINodes := make([]torus.INodeRef, 0, len(snaps)+1)
	curINodes = append(curINodes, curRef)
	for _, x := range snaps {
		curINodes = append(curINodes, torus.INodeRefFromBytes(x.INodeRef))
	}

	for _, x := range curINodes {
		inode, err := b.inodes.GetINode(b.getContext(), x)
		if err != nil {
			return err
		}
		set, err := blockset.UnmarshalFromProto(inode.Blocks, nil)
		if err != nil {
			return err
		}
		refs := set.GetAllBlockRefs()
		for _, ref := range refs {
			if ref.IsZero() {
				continue
			}
			if ref.INode > b.highwaters[ref.Volume()] {
				b.highwaters[ref.Volume()] = ref.INode
			}
			b.set[ref] = true
		}
	}
	b.curINodes = append(b.curINodes, curINodes...)
	return nil
}

func (b *blockvolGC) IsDead(ref torus.BlockRef) bool {
	v, ok := b.highwaters[ref.Volume()]
	if !ok {
		if clog.LevelAt(capnslog.TRACE) {
			clog.Tracef("%s doesn't exist anymore", ref)
		}
		// Volume doesn't exist anymore
		return true
	}
	// If it's a new block or INode, let it be.
	if ref.INode >= v {
		if clog.LevelAt(capnslog.TRACE) {
			clog.Tracef("%s is new compared to %d", ref, v)
		}
		return false
	}
	// If it's an INode block, and it's not in our list
	if ref.BlockType() == torus.TypeINode {
		for _, x := range b.curINodes {
			if ref.HasINode(x, torus.TypeINode) {
				if clog.LevelAt(capnslog.TRACE) {
					clog.Tracef("%s is in %s", ref, x)
				}
				return false
			}
		}
		if clog.LevelAt(capnslog.TRACE) {
			clog.Tracef("%s is a dead INode", ref)
		}
		return true
	}
	// If it's a data block
	if v := b.set[ref]; v {
		return false
	}
	if clog.LevelAt(capnslog.TRACE) {
		clog.Tracef("%s is dead", ref)
	}
	return true
}

func (b *blockvolGC) Clear() {
	b.highwaters = make(map[torus.VolumeID]torus.INodeID)
	b.curINodes = make([]torus.INodeRef, 0, len(b.curINodes))
	b.set = make(map[torus.BlockRef]bool)
}
