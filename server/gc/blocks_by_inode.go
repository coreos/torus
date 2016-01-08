package gc

import (
	"sync"
	"time"

	"github.com/coreos/agro"
)

type blocksByINode struct {
	mut       sync.Mutex
	mds       agro.MetadataService
	blocks    agro.BlockStore
	stopChan  chan bool
	forceChan chan bool
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
	b.stopChan = sc
	b.forceChan = fc
	go blocksByINodeMain(b, sc, fc)
}

func (b *blocksByINode) Stop() {
	close(b.stopChan)
}

func (b *blocksByINode) Force() {
	b.forceChan <- true
}

func (b *blocksByINode) LastComplete() time.Time {
	return b.last
}

func (b *blocksByINode) gc(volume string) {

}

func blocksByINodeMain(b *blocksByINode, stop chan bool, force chan bool) {
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
	}
	clog.Debug("bbi: ending blocksByInode")
}
