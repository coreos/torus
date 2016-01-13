package server

import (
	"errors"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

func init() {
	rebalancerRegistry[Replace] = func() Rebalancer {
		return &replace{}
	}
}

type replace struct{}

func (r *replace) makeMessage(phase int32, d *distributor) *models.RebalanceStatus {
	return &models.RebalanceStatus{
		RebalanceType: int32(Replace),
		Phase:         phase,
		FromLeader:    true,
		UUID:          d.UUID(),
	}
}

func (r *replace) Leader(d *distributor, inOut [2]chan *models.RebalanceStatus, newring agro.Ring) {
	in, out := inOut[0], inOut[1]
	out <- r.makeMessage(1, d)
	waitAll(in, newring, 1)
}

func (r *replace) AdvanceState(d *distributor, s *models.RebalanceStatus, newring agro.Ring) (*models.RebalanceStatus, bool, error) {
	if s.Phase != 1 {
		return nil, true, errors.New("unknown phase")
	}
	s.FromLeader = false
	return s, true, nil
}

func (r *replace) OnError(err error) *models.RebalanceStatus {
	clog.Error(err)
	return nil
}

func (r *replace) Timeout() { return }
