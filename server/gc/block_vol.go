package gc

import (
	"sync"

	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/models"
	"github.com/hashicorp/go-immutable-radix"
)

type blockvolGC struct {
	mut       sync.Mutex
	mds       agro.BlockMetadataService
	trie      *iradix.Tree
	highwater agro.INodeID
	skip      bool
}

func NewBlockVolGC(mds agro.MetadataService) GC {
	if m, ok := mds.(agro.BlockMetadataService); ok {
		return &blockvolGC{mds: m}
	}
	return &nullGC{}
}

func (b *blockvolGC) PrepVolume(vol *models.Volume) error {
	b.mut.Lock()
	defer b.mut.Unlock()
	t := iradix.New()
	b.skip = false
	if vol.Type != models.Volume_BLOCK {
		b.skip = true
		return nil
	}
	inode, err := b.mds.GetBlockVolumeINode(agro.VolumeID(vol.Id))
	if err != nil {
		return err
	}
	set, err := blockset.UnmarshalFromProto(inode.Blocks, nil)
	if err != nil {
		return err
	}
	tx := t.Txn()
	refs := set.GetAllBlockRefs()
	for _, ref := range refs {
		if ref.IsZero() {
			continue
		}
		if ref.INode > b.highwater {
			b.highwater = ref.INode
		}
		tx.Insert(ref.ToBytes(), true)
	}
	b.trie = tx.Commit()
	return nil
}

func (b *blockvolGC) IsDead(ref agro.BlockRef) bool {
	b.mut.Lock()
	defer b.mut.Unlock()
	if b.skip {
		return false
	}
	if ref.INode > b.highwater {
		return false
	}
	if _, ok := b.trie.Get(ref.ToBytes()); ok {
		return false
	}
	clog.Tracef("%s is dead", ref)
	return true
}

func (b *blockvolGC) Clear() {}
