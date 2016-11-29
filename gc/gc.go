// gc provides the Torus interface for how garbage collection is implemented.
// Volumes implement a garbage collector and the controller in this package runs
// them.
package gc

import (
	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
	"golang.org/x/net/context"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/torus", "gc")

type controller struct {
	gcs []GC
}

type GC interface {
	PrepVolume(*models.Volume) error
	IsDead(torus.BlockRef) bool
	Clear()
}

type INodeFetcher interface {
	GetINode(context.Context, torus.INodeRef) (*models.INode, error)
}

func NewGCController(srv *torus.Server, inodes INodeFetcher) GC {
	var gcs []GC
	for k, v := range gcFuncs {
		clog.Debugf("creating %s gc", k)
		gc, err := v(srv, inodes)
		if err != nil {
			clog.Errorf("cannot create gc %s: %v", k, err)
			continue
		}
		gcs = append(gcs, gc)
	}
	return &controller{
		gcs: gcs,
	}
}

func (c *controller) PrepVolume(vol *models.Volume) error {
	n := 0
	for _, x := range c.gcs {
		n++
		err := x.PrepVolume(vol)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *controller) IsDead(ref torus.BlockRef) bool {
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

type CreateGCFunc func(srv *torus.Server, inodes INodeFetcher) (GC, error)

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
