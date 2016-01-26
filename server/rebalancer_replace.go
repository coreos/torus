package server

import (
	"errors"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

func init() {
	rebalancerRegistry[Replace] = func(d *distributor, newRing agro.Ring) Rebalancer {
		return &replace{
			d:       d,
			newRing: newRing,
		}
	}
}

type replace struct {
	d       *distributor
	newRing agro.Ring
}

func (r *replace) makeMessage(phase int32) *models.RebalanceStatus {
	return &models.RebalanceStatus{
		RebalanceType: int32(Replace),
		Phase:         phase,
		FromLeader:    true,
		UUID:          r.d.UUID(),
	}
}

func (r *replace) Leader(inOut [2]chan *models.RebalanceStatus) {
	in, out := inOut[0], inOut[1]
	out <- r.makeMessage(1)
	waitAll(in, r.newRing.Members(), 1)
}

func (r *replace) AdvanceState(s *models.RebalanceStatus) (*models.RebalanceStatus, bool, error) {
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

func (r *replace) RebalanceMessage(context.Context, *models.RebalanceRequest) (*models.RebalanceResponse, error) {
	panic("somehow got a message while rebalancing a replace")
}
