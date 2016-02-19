package gc

import (
	"sync"
	"time"

	"github.com/tylertreat/BoomFilters"
	"golang.org/x/net/context"

	"github.com/coreos/agro"
)

type blocksByINode struct {
	mut        sync.Mutex
	mds        agro.MetadataService
	blocks     agro.BlockStore
	stopChan   chan bool
	forceChan  chan bool
	doneChan   chan bool
	last       time.Time
	gcBloom    *boom.InverseBloomFilter
	bloomSize  uint
	bloomCount uint
}

const bbiBloomGCSize = 300 * 1024 * 1024 * 1024 //100GiB

func NewBlocksByINodeGC(mds agro.MetadataService, blocks agro.BlockStore) GC {
	size := uint(bbiBloomGCSize / blocks.BlockSize())
	return &blocksByINode{
		mds:       mds,
		blocks:    blocks,
		last:      time.Unix(0, 0),
		gcBloom:   boom.NewInverseBloomFilter(size),
		bloomSize: size,
	}
}

func (b *blocksByINode) Start() {
	if b.stopChan != nil {
		return
	}
	sc := make(chan bool)
	fc := make(chan bool)
	dc := make(chan bool)
	b.stopChan = sc
	b.forceChan = fc
	b.doneChan = dc
	go blocksByINodeMain(b, sc, fc, dc)
}

func (b *blocksByINode) Stop() {
	if b.stopChan == nil {
		return
	}
	close(b.stopChan)
	close(b.forceChan)
	<-b.doneChan
	b.stopChan = nil
	b.doneChan = nil
	b.forceChan = nil
}

func (b *blocksByINode) Force() {
	if b.forceChan != nil {
		b.forceChan <- true
	}
}

func (b *blocksByINode) LastComplete() time.Time {
	return b.last
}

func (b *blocksByINode) RecentlyGCed(ref agro.BlockRef) bool {
	ok := b.gcBloom.Test(ref.ToBytes())
	if ok {
		return true
	}
	deadmap, held, err := b.mds.GetVolumeLiveness(ref.Volume())
	if err != nil {
		return false
	}
	for _, x := range held {
		deadmap.AndNot(x)
	}
	if deadmap.Contains(uint32(ref.INode)) {
		b.gcBloom.Add(ref.ToBytes())
		return true
	}
	return false
}

func (b *blocksByINode) gc(volume string) {
	vid, err := b.mds.GetVolumeID(volume)
	if err != nil {
		clog.Errorf("bbi: got error getting volume ID for %s %v", volume, err)
	}
	deadmap, held, err := b.mds.GetVolumeLiveness(vid)
	if err != nil {
		clog.Errorf("bbi: got error gcing volume %s %v", volume, err)
	}
	for _, x := range held {
		deadmap.AndNot(x)
	}
	it := b.blocks.BlockIterator()
	for it.Next() {
		ref := it.BlockRef()
		if ref.Volume() == vid && deadmap.Contains(uint32(ref.INode)) {
			b.blocks.DeleteBlock(context.TODO(), ref)
			b.gcBloom.Add(ref.ToBytes())
			b.bloomCount++
		}
	}
	err = it.Err()
	if err != nil {
		clog.Errorf("bbi: error in blockref iteration")
	}
	if b.bloomCount >= b.bloomSize {
		// Flush it if it's getting too full
		b.gcBloom = boom.NewInverseBloomFilter(b.bloomSize)
		b.bloomCount = 0
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
