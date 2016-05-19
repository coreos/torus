package block

import (
	"time"

	"golang.org/x/net/context"

	"github.com/betawaffle/trie"
	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/gc"
	"github.com/coreos/agro/models"
	"github.com/coreos/pkg/capnslog"
)

func init() {
	gc.RegisterGC("blockvol", NewBlockVolGC)
}

type blockvolGC struct {
	srv        *agro.Server
	inodes     gc.INodeFetcher
	trie       *trie.Node
	highwaters map[agro.VolumeID]agro.INodeID
	curINodes  []agro.INodeRef
}

func NewBlockVolGC(srv *agro.Server, inodes gc.INodeFetcher) (gc.GC, error) {
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
	mds, err := createBlockMetadata(b.srv.MDS, vol.Name, agro.VolumeID(vol.Id))
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

	curINodes := []agro.INodeRef{curRef}

	snaps, err := mds.GetSnapshots()
	if err != nil {
		return err
	}

	for _, x := range snaps {
		curINodes = append(curINodes, agro.INodeRefFromBytes(x.INodeRef))
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
		tx := new(trie.Txn)
		refs := set.GetAllBlockRefs()
		tx.Prealloc(len(refs))
		for _, ref := range refs {
			if ref.IsZero() {
				continue
			}
			if ref.INode > b.highwaters[ref.Volume()] {
				b.highwaters[ref.Volume()] = ref.INode
			}
			tx.Put(ref.ToBytes(), true)
		}
		tx.Merge(b.trie)
		b.trie = tx.Commit()
	}
	b.curINodes = append(b.curINodes, curINodes...)
	return nil
}

func (b *blockvolGC) IsDead(ref agro.BlockRef) bool {
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
	if ref.BlockType() == agro.TypeINode {
		for _, x := range b.curINodes {
			if ref.HasINode(x, agro.TypeINode) {
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
	if v, ok := b.trie.Get(ref.ToBytes()).(bool); v && ok {
		return false
	}
	if clog.LevelAt(capnslog.TRACE) {
		clog.Tracef("%s is dead", ref)
	}
	return true
}

func (b *blockvolGC) Clear() {
	b.highwaters = make(map[agro.VolumeID]agro.INodeID)
	b.curINodes = make([]agro.INodeRef, 0, len(b.curINodes))
	b.trie = nil
}
