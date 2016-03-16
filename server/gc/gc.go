package gc

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/coreos/pkg/capnslog"
	"golang.org/x/net/context"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "gc")

type controller struct {
	gcs []GC
}

type GC interface {
	PrepVolume(*models.Volume) error
	IsDead(agro.BlockRef) bool
	Clear()
}

type INodeFetcher interface {
	GetINode(context.Context, agro.INodeRef) (*models.INode, error)
}

func NewGCController(mds agro.MetadataService, inodes INodeFetcher) GC {
	return &controller{
		gcs: []GC{NewBlocksByINodeGC(mds), NewDeadINodeGC(mds), NewBlockVolGC(mds, inodes)},
	}
}

func (c *controller) PrepVolume(vol *models.Volume) error {
	for _, x := range c.gcs {
		err := x.PrepVolume(vol)
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
