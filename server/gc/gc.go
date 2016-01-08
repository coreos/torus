package gc

import (
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/pkg/capnslog"
)

const (
	DefaultGCWait = 5 * time.Second
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "gc")

type controller struct {
	blocks agro.BlockStore
	mds    agro.MetadataService
	gcs    []GC
}

type GC interface {
	Start()
	Stop()
	Force()
	LastComplete() time.Time
}

func NewGCController(mds agro.MetadataService, blocks agro.BlockStore) GC {
	return &controller{
		blocks: blocks,
		mds:    mds,
		gcs:    []GC{NewBlocksByINodeGC(mds, blocks)},
	}
}

func (c *controller) Start() {
	for _, x := range c.gcs {
		x.Start()
	}
}

func (c *controller) Stop() {
	for _, x := range c.gcs {
		x.Stop()
	}
}

func (c *controller) Force() {
	for _, x := range c.gcs {
		x.Force()
	}
}

func (c *controller) LastComplete() time.Time {
	earliest := time.Unix(0, 0)
	for _, x := range c.gcs {
		n := x.LastComplete()
		if n.Before(earliest) {
			earliest = n
		}
	}
	return earliest
}
