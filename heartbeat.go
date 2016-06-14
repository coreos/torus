package torus

import (
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/coreos/torus/models"
	"github.com/prometheus/client_golang/prometheus"

	"golang.org/x/net/context"
)

const (
	heartbeatTimeout  = 1 * time.Second
	heartbeatInterval = 5 * time.Second
)

var (
	promHeartbeats = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "torus_server_heartbeats",
		Help: "Number of times this server has heartbeated to mds",
	})
	promServerPeers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "torus_server_peers_total",
		Help: "Number of peers this server sees",
	})
)

func init() {
	prometheus.MustRegister(promHeartbeats)
	prometheus.MustRegister(promServerPeers)
}

// BeginHeartbeat spawns a goroutine for heartbeats. Non-blocking.
func (s *Server) BeginHeartbeat(addr *url.URL) error {
	if s.heartbeating {
		return nil
	}
	if addr != nil {
		ipaddr, port, err := net.SplitHostPort(addr.Host)
		if err != nil {
			return err
		}
		ipaddr = autodetectIP(ipaddr)
		advertiseURI := *addr
		advertiseURI.Host = ipaddr
		if port != "" {
			advertiseURI.Host = fmt.Sprintf("%s:%s", ipaddr, port)
		}
		s.peerInfo.Address = advertiseURI.String()
	}
	var err error
	s.lease, err = s.MDS.GetLease()
	if err != nil {
		return err
	}
	s.UpdateRebalanceInfo(&models.RebalanceInfo{})
	ch := make(chan interface{})
	s.closeChans = append(s.closeChans, ch)
	go s.heartbeat(ch)
	s.heartbeating = true
	return nil
}

func (s *Server) heartbeat(cl chan interface{}) {
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

func (s *Server) AddTimeoutCallback(f func(uuid string)) {
	s.timeoutCallbacks = append(s.timeoutCallbacks, f)
}

func (s *Server) oneHeartbeat() {
	promHeartbeats.Inc()

	s.mut.Lock()
	s.peerInfo.TotalBlocks = s.Blocks.NumBlocks()
	s.peerInfo.UsedBlocks = s.Blocks.UsedBlocks()
	s.mut.Unlock()

	s.infoMut.Lock()
	defer s.infoMut.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatTimeout)
	defer cancel()
	err := s.MDS.WithContext(ctx).RegisterPeer(s.lease, s.peerInfo)
	if err != nil {
		clog.Warningf("couldn't register heartbeat: %s", err)
	}
	s.updatePeerMap()
}

func (s *Server) updatePeerMap() {
	ctxget, cancelget := context.WithTimeout(context.Background(), heartbeatTimeout)
	defer cancelget()
	peers, err := s.MDS.WithContext(ctxget).GetPeers()
	if err != nil {
		clog.Warningf("couldn't update peerlist: %s", err)
	}
	promServerPeers.Set(float64(len(peers)))

	s.mut.Lock()
	defer s.mut.Unlock()

	for _, p := range peers {
		s.peersMap[p.UUID] = p
	}
	for k := range s.peersMap {
		found := false
		for _, p := range peers {
			if p.UUID == k {
				found = true
				break
			}
		}
		if !found {
			for _, f := range s.timeoutCallbacks {
				f(k)
			}
			s.peersMap[k].TimedOut = true
		}
	}
}

func (s *Server) UpdatePeerMap() map[string]*models.PeerInfo {
	s.updatePeerMap()
	return s.GetPeerMap()
}

func (s *Server) UpdateRebalanceInfo(ri *models.RebalanceInfo) {
	s.infoMut.Lock()
	defer s.infoMut.Unlock()
	s.peerInfo.RebalanceInfo = ri
}

func autodetectIP(ip string) string {
	// We can't advertise "all IPs"
	if ip != "0.0.0.0" {
		return ip
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic(err)
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	// Just do localhost.
	return "127.0.0.1"
}
