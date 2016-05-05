package distributor

import (
	"io"
	"math/rand"
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

// Goroutine which watches for new rings and kicks off
// the rebalance dance.
func (d *Distributor) ringWatcher(closer chan struct{}) {
	ch := make(chan agro.Ring)
	d.srv.MDS.SubscribeNewRings(ch)
exit:
	for {
		select {
		case <-closer:
			d.srv.MDS.UnsubscribeNewRings(ch)
			close(ch)
			break exit
		case newring, ok := <-ch:
			if ok {
				if newring.Version() == d.ring.Version() {
					// No problem. We're seeing the same ring.
					continue
				}
				if newring.Version() != d.ring.Version()+1 {
					panic("replacing old ring with ring in the far future!")
				}
				d.mut.Lock()
				d.ring = newring
				d.mut.Unlock()
			} else {
				break exit
			}
		}
	}
}

func (d *Distributor) rebalanceTicker(closer chan struct{}) {
	n := 0
	total := 0
	time.Sleep(time.Duration(250+rand.Intn(250)) * time.Millisecond)
	volIdx := 0
	var volset []*models.Volume
	var volhighwater agro.VolumeID
	var err error
exit:
	for {
		if volIdx == len(volset) {
			err = d.rebalancer.DeleteUnusedVolumes(volset, volhighwater)
			if err != nil {
				clog.Error(err)
			}
			volIdx = 0
			volset, volhighwater, err = d.srv.MDS.GetVolumes()
			if err != nil {
				clog.Error(err)
			}
			time.Sleep(time.Duration(rand.Intn(250)) * time.Millisecond)
			continue
		}
		err := d.rebalancer.UseVolume(volset[volIdx])
		if err != nil {
			clog.Errorf("gc prep failed: %s", err)
		}
		clog.Tracef("starting rebalance/gc for %s", volset[volIdx])
	volume:
		for {
			timeout := 2 * time.Duration(n+1) * time.Millisecond
			select {
			case <-closer:
				break exit
			case <-time.After(timeout):
				written, err := d.rebalancer.Tick()
				if d.ring.Version() != d.rebalancer.VersionStart() {
					// Something is changed -- we are now rebalancing
					d.rebalancing = true
				}
				info := &models.RebalanceInfo{
					Rebalancing: d.rebalancing,
				}
				total += written
				info.LastRebalanceBlocks = uint64(total)
				if err == io.EOF {
					// Good job, sleep well, I'll most likely rebalance you in the morning.
					info.LastRebalanceFinish = time.Now().UnixNano()
					total = 0
					finishver := d.rebalancer.VersionStart()
					if finishver == d.ring.Version() {
						d.rebalancing = false
						info.Rebalancing = false
					}
					d.srv.UpdateRebalanceInfo(info)
					break volume
				} else if err != nil {
					// This is usually really bad
					clog.Error(err)
				}
				n = written
				clog.Trace(n)
				d.srv.UpdateRebalanceInfo(info)
			}
		}
		time.Sleep(time.Duration(rand.Intn(3)) * time.Second)
		volIdx++
	}
}
