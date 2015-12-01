package server

import (
	"time"

	"github.com/barakmich/agro/models"

	"golang.org/x/net/context"
)

const (
	heartbeatTimeout  = 1 * time.Second
	heartbeatInterval = 10 * time.Second
)

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
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatTimeout)
	defer cancel()
	p := &models.PeerInfo{}
	p.Address = s.internalAddr
	p.TotalBlocks = s.blocks.NumBlocks()
	p.UsedBlocks = s.blocks.UsedBlocks()
	p.UUID = s.mds.UUID()
	err := s.mds.WithContext(ctx).RegisterPeer(p)
	if err != nil {
		clog.Warningf("couldn't register heartbeat: %s", err)
	}
	ctxget, cancelget := context.WithTimeout(context.Background(), heartbeatTimeout)
	defer cancelget()
	peers, err := s.mds.WithContext(ctxget).GetPeers()
	if err != nil {
		clog.Warningf("couldn't update peerlist: %s", err)
	}
	s.updatePeerMap(peers)
}

func (s *server) updatePeerMap(peers []*models.PeerInfo) {
	for _, p := range peers {
		s.peersMap[p.UUID] = p
	}
}
