package rebalance

import (
	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/torus"
	"github.com/coreos/torus/gc"
	"github.com/coreos/torus/models"
	"golang.org/x/net/context"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/torus", "rebalance")

type Ringer interface {
	Ring() torus.Ring
	UUID() string
}

type Rebalancer interface {
	Tick() (int, error)
	VersionStart() int
	PrepVolume(*models.Volume) error
	Reset() error
}

type CheckAndSender interface {
	Check(ctx context.Context, peer string, refs []torus.BlockRef) ([]bool, error)
	PutBlock(ctx context.Context, peer string, ref torus.BlockRef, data []byte) error
}

func NewRebalancer(r Ringer, bs torus.BlockStore, cs CheckAndSender, gc gc.GC) Rebalancer {
	return &rebalancer{
		r:  r,
		bs: bs,
		cs: cs,
		gc: gc,
	}
}

type rebalancer struct {
	r    Ringer
	bs   torus.BlockStore
	cs   CheckAndSender
	it   torus.BlockIterator
	gc   gc.GC
	ring torus.Ring
}

func (r *rebalancer) VersionStart() int {
	if r.ring == nil {
		return r.r.Ring().Version()
	}
	return r.ring.Version()
}

func (r *rebalancer) PrepVolume(vol *models.Volume) error {
	return r.gc.PrepVolume(vol)
}

func (r *rebalancer) Reset() error {
	if r.it != nil {
		r.it.Close()
		r.it = nil
	}
	r.gc.Clear()
	return nil
}
