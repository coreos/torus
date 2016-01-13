package gc

import (
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
)

type blocksByINode struct {
	mut       sync.Mutex
	mds       agro.MetadataService
	blocks    agro.BlockStore
	stopChan  chan bool
	forceChan chan bool
	doneChan  chan bool
	last      time.Time
}

func NewBlocksByINodeGC(mds agro.MetadataService, blocks agro.BlockStore) GC {
	return &blocksByINode{
		mds:    mds,
		blocks: blocks,
		last:   time.Unix(0, 0),
	}
}

func (b *blocksByINode) Start() {
	sc := make(chan bool)
	fc := make(chan bool)
	dc := make(chan bool)
	b.stopChan = sc
	b.forceChan = fc
	b.doneChan = dc
	go blocksByINodeMain(b, sc, fc, dc)
}

func (b *blocksByINode) Stop() {
	close(b.stopChan)
	<-b.doneChan
}

func (b *blocksByINode) Force() {
	b.forceChan <- true
}

func (b *blocksByINode) LastComplete() time.Time {
	return b.last
}

func (b *blocksByINode) gc(volume string) {
	vid, err := b.mds.GetVolumeID(volume)
	if err != nil {
		clog.Errorf("bbi: got error getting volume ID for %s %v", volume, err)
	}
	deadmap, held, err := b.mds.GetVolumeLiveness(volume)
	if err != nil {
		clog.Errorf("bbi: got error gcing volume %s %v", volume, err)
	}
	for _, x := range held {
		deadmap.AndNot(x)
	}
	it := deadmap.Iterator()
	for it.HasNext() {
		i := it.Next()
		ref := agro.INodeRef{
			Volume: vid,
			INode:  agro.INodeID(i),
		}
		err := b.blocks.DeleteINodeBlocks(context.TODO(), ref)
		if err != nil {
			clog.Errorf("bbi: got error gcing volume %s deleting INode %v: %v", volume, ref, err)
		}
	}
}

func blocksByINodeMain(b *blocksByINode, stop chan bool, force chan bool, done chan bool) {
	clog.Debug("bbi: starting blocksByInode")
all:
	for {
		forced := false
		select {
		case <-time.After(DefaultGCWait):
			clog.Trace("bbi: top of gc")
		case <-stop:
			break all
		case <-force:
			forced = true
			clog.Debug("bbi: forcing")
		}
		volumes, err := b.mds.GetVolumes()
		if err != nil {
			clog.Error("couldn't get volumes", err)
		}
		for _, v := range volumes {
			b.gc(v)
			if forced {
				continue
			}
			select {
			case <-time.After(DefaultGCWait):
			case <-stop:
				break all
			}
		}
		b.last = time.Now()
	}
	clog.Debug("bbi: ending blocksByInode")
	close(done)
}
