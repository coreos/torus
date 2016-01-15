package server

import (
	"encoding/json"
	"errors"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

func init() {
	rebalancerRegistry[Full] = func(d *distributor, newRing agro.Ring) Rebalancer {
		return &full{
			d:       d,
			newRing: newRing,
		}
	}
}

type full struct {
	d        *distributor
	newRing  agro.Ring
	locked   bool
	replaced bool
	state    map[string]agro.INodeID
}

func (f *full) leaderMessage(phase int32) *models.RebalanceStatus {
	s := f.makeMessage(phase)
	s.FromLeader = true
	return s
}

func (f *full) makeMessage(phase int32) *models.RebalanceStatus {
	return &models.RebalanceStatus{
		RebalanceType: int32(Replace),
		Phase:         phase,
		UUID:          f.d.UUID(),
	}
}

func (f *full) abort(out chan *models.RebalanceStatus, phase int32, err error) {
	rlog.Error(err)
	rlog.Errorf("state %d, safe to abort", phase)
	out <- f.leaderMessage(-1)
	f.Timeout()
}

func (f *full) Leader(inOut [2]chan *models.RebalanceStatus) {
	in, out := inOut[0], inOut[1]
	// Phase 1: Everyone stop printing inodes.
	out <- f.leaderMessage(1)
	f.doState(1)
	waitAll(in, f.newRing, 1)
	// Phase 2: Capture the inode map, save it as the rebalance state.
	//          Everyone else grabs this, injects, and goes again.
	state, err := f.d.srv.mds.GetINodeIndexes()
	if err != nil {
		f.abort(out, 1, err)
		return
	}
	b, err := json.Marshal(state)
	if err != nil {
		f.abort(out, 1, err)
		return
	}
	err = f.d.srv.mds.SetRebalanceSnapshot(uint64(Replace), b)
	out <- f.leaderMessage(2)
	f.doState(2)
	waitAll(in, f.newRing, 2)
	// Phase 3: Begin copying! (Until complete)
	// Phase 4: Replace the ring and remove self from storage
}

func (f *full) AdvanceState(s *models.RebalanceStatus) (*models.RebalanceStatus, bool, error) {
	ok := f.doState(s.Phase)
	if !ok {
		// Abort
		f.Timeout()
		return nil, true, errors.New("aborting")
	}
	if s.Phase == 4 {
		return f.makeMessage(s.Phase), true, nil
	}
	return f.makeMessage(s.Phase), false, nil
}

func (f *full) doState(phase int32) bool {
	switch phase {
	case 1:
		f.d.srv.gc.Stop()
		f.d.srv.writeableLock.Lock()
		f.locked = true
	case 2:
		_, b, err := f.d.srv.mds.GetRebalanceSnapshot()
		if err != nil {
			rlog.Error(err)
			f.Timeout()
			return false
		}
		err = json.Unmarshal(b, &f.state)
		if err != nil {
			rlog.Error(err)
			f.Timeout()
			return false
		}
		f.d.mut.Lock()
		defer f.d.mut.Unlock()

		f.d.srv.writeableLock.Unlock()
		f.locked = false
	default:
		return false
	}
	return true
}

func (f *full) OnError(err error) *models.RebalanceStatus {
	return nil
}

func (f *full) Timeout() {
	if f.locked {
		f.d.srv.writeableLock.Unlock()
		f.locked = false
	}
	if f.replaced {
		f.d.mut.Lock()
		defer f.d.mut.Unlock()
	}
	return
}
