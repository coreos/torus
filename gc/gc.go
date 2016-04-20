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

func NewGCController(srv *agro.Server, inodes INodeFetcher) GC {
	var gcs []GC
	for k, v := range gcFuncs {
		clog.Debugf("creating %s gc", k)
		gc, err := v(srv, inodes)
		if err != nil {
			clog.Errorf("cannot create gc %s", k)
			continue
		}
		gcs = append(gcs, gc)
	}
	return &controller{
		gcs: gcs,
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

type CreateGCFunc func(srv *agro.Server, inodes INodeFetcher) (GC, error)

var gcFuncs map[string]CreateGCFunc

func RegisterGC(name string, newFunc CreateGCFunc) {
	if gcFuncs == nil {
		gcFuncs = make(map[string]CreateGCFunc)
	}

	if _, ok := gcFuncs[name]; ok {
		panic("gc: attempted to register GC " + name + " twice")
	}

	gcFuncs[name] = newFunc
}
