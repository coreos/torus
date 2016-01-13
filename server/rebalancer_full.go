package server

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

func init() {
	rebalancerRegistry[Full] = func() Rebalancer {
		return &full{}
	}
}

type full struct{}

func (f *full) makeMessage(phase int32, d *distributor) *models.RebalanceStatus {
	return &models.RebalanceStatus{
		RebalanceType: int32(Replace),
		Phase:         phase,
		FromLeader:    true,
		UUID:          d.UUID(),
	}
}

func (f *full) Leader(d *distributor, inOut [2]chan *models.RebalanceStatus, newring agro.Ring) {
	in, out := inOut[0], inOut[1]
	// Phase 1: Everyone stop printing inodes.
	out <- f.makeMessage(1, d)
	waitAll(in, newring, 1)
	// Phase 2: Capture the inode map, save it as the rebalance state.
	//          Everyone else grabs this, injects, and goes again.
	// Phase 3: Begin copying! (Until complete)
	// Phase 4: Replace the ring and remove self from storage
}

func (f *full) AdvanceState(d *distributor, s *models.RebalanceStatus, newring agro.Ring) (*models.RebalanceStatus, bool, error) {
	return nil, true, nil
}

func (f *full) OnError(err error) *models.RebalanceStatus {
	return nil
}

func (f *full) Timeout() {
	return
}
