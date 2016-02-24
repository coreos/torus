package server

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"golang.org/x/net/context"
)

const (
	heartbeatTimeout  = 1 * time.Second
	heartbeatInterval = 5 * time.Second
)

var (
	promHeartbeats = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_server_heartbeats",
		Help: "Number of times this server has heartbeated to mds",
	})
	promServerPeers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "agro_server_peers_total",
		Help: "Number of peers this server sees",
	})
)

func init() {
	prometheus.MustRegister(promHeartbeats)
	prometheus.MustRegister(promServerPeers)
}

func (s *server) BeginHeartbeat() error {
	if s.heartbeating {
		return nil
	}
	ch := make(chan interface{})
	s.closeChans = append(s.closeChans, ch)
	go s.heartbeat(ch)
	s.heartbeating = true
	return nil
}

func (s *server) heartbeat(cl chan interface{}) {
	for {
		s.oneHeartbeat()
		select {
		case <-cl:
			// TODO(barakmich): Clean up.
			return
		case <-time.After(heartbeatInterval):
			clog.Trace("heartbeating again")
		}
	}
}

func (s *server) oneHeartbeat() {
	promHeartbeats.Inc()
	s.infoMut.Lock()
	defer s.infoMut.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatTimeout)
	defer cancel()
	s.peerInfo.TotalBlocks = s.blocks.NumBlocks()
	s.peerInfo.UsedBlocks = s.blocks.UsedBlocks()
	err := s.mds.WithContext(ctx).RegisterPeer(s.peerInfo)
	if err != nil {
		clog.Warningf("couldn't register heartbeat: %s", err)
	}
	s.updatePeerMap()
}

func (s *server) updatePeerMap() {
	ctxget, cancelget := context.WithTimeout(context.Background(), heartbeatTimeout)
	defer cancelget()
	peers, err := s.mds.WithContext(ctxget).GetPeers()
	if err != nil {
		clog.Warningf("couldn't update peerlist: %s", err)
	}
	s.mut.Lock()
	defer s.mut.Unlock()
	promServerPeers.Set(float64(len(peers)))
	for _, p := range peers {
		s.peersMap[p.UUID] = p
	}
}
