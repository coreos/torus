package gc

import (
	"github.com/coreos/agro"
	"github.com/coreos/pkg/capnslog"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "gc")

type controller struct {
	gcs []GC
}

type GC interface {
	PrepVolume(agro.VolumeID) error
	IsDead(agro.BlockRef) bool
	Clear()
}

func NewGCController(mds agro.MetadataService) GC {
	return &controller{
		gcs: []GC{NewBlocksByINodeGC(mds), NewDeadINodeGC(mds)},
	}
}

func (c *controller) PrepVolume(vid agro.VolumeID) error {
	for _, x := range c.gcs {
		err := x.PrepVolume(vid)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *controller) IsDead(ref agro.BlockRef) bool {
	for _, x := range c.gcs {
		if x.IsDead(ref) {
			return true
		}
	}
	return false
}

func (c *controller) Clear() {
	for _, x := range c.gcs {
		x.Clear()
	}
}
