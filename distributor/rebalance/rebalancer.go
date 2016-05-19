package rebalance

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/gc"
	"github.com/coreos/agro/models"
	"github.com/coreos/pkg/capnslog"
	"golang.org/x/net/context"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "rebalance")

type Ringer interface {
	Ring() agro.Ring
	UUID() string
}

type Rebalancer interface {
	Tick() (int, error)
	VersionStart() int
	PrepVolume(*models.Volume) error
	Reset() error
}

type CheckAndSender interface {
	Check(ctx context.Context, peer string, refs []agro.BlockRef) ([]bool, error)
	PutBlock(ctx context.Context, peer string, ref agro.BlockRef, data []byte) error
}

func NewRebalancer(r Ringer, bs agro.BlockStore, cs CheckAndSender, gc gc.GC) Rebalancer {
	return &rebalancer{
		r:  r,
		bs: bs,
		cs: cs,
		gc: gc,
	}
}

type rebalancer struct {
	r    Ringer
	bs   agro.BlockStore
	cs   CheckAndSender
	it   agro.BlockIterator
	gc   gc.GC
	ring agro.Ring
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
