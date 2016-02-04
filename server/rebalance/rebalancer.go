package rebalance

import (
	"github.com/coreos/agro"
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
}

type CheckAndSender interface {
	Check(ctx context.Context, peer string, refs []agro.BlockRef) ([]bool, error)
	PutBlock(ctx context.Context, peer string, ref agro.BlockRef, data []byte) error
}

func NewRebalancer(r Ringer, bs agro.BlockStore, cs CheckAndSender) Rebalancer {
	return &rebalancer{
		r:       r,
		bs:      bs,
		cs:      cs,
		version: 0,
	}
}

type rebalancer struct {
	r       Ringer
	bs      agro.BlockStore
	cs      CheckAndSender
	it      agro.BlockIterator
	version int
}

func (r *rebalancer) VersionStart() int {
	if r.version == 0 {
		return r.r.Ring().Version()
	}
	return r.version
}
