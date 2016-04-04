package rebalance

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/server/gc"
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
	UseVolume(*models.Volume) error
}

type CheckAndSender interface {
	Check(ctx context.Context, peer string, refs []agro.BlockRef) ([]bool, error)
	PutBlock(ctx context.Context, peer string, ref agro.BlockRef, data []byte) error
}

func NewRebalancer(r Ringer, bs agro.BlockStore, cs CheckAndSender, gc gc.GC) Rebalancer {
	return &rebalancer{
		r:       r,
		bs:      bs,
		cs:      cs,
		gc:      gc,
		vol:     nil,
		version: 0,
	}
}

type rebalancer struct {
	r       Ringer
	bs      agro.BlockStore
	cs      CheckAndSender
	it      agro.BlockIterator
	gc      gc.GC
	vol     *models.Volume
	version int
}

func (r *rebalancer) VersionStart() int {
	if r.version == 0 {
		return r.r.Ring().Version()
	}
	return r.version
}

func (r *rebalancer) UseVolume(vol *models.Volume) error {
	r.vol = vol
	r.gc.Clear()
	return r.gc.PrepVolume(vol)
}
