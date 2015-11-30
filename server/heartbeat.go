package server

import (
	"time"

	"github.com/barakmich/agro/models"

	"golang.org/x/net/context"
)

const (
	heartbeatTimeout = 1 * time.Second
)

func (s *server) BeginHeartbeat() error {
	ch := make(chan interface{})
	s.closeChans = append(s.closeChans, ch)
	go s.heartbeat(ch)
	return nil
}

func (s *server) heartbeat(cl chan interface{}) {
	for {
		s.oneHeartbeat()
		select {
		case <-cl:
			// TODO(barakmich): Clean up.
			return
		case <-time.After(heartbeatTimeout):
			clog.Trace("heartbeating again")
		}
	}
}

func (s *server) oneHeartbeat() {
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatTimeout)
	defer cancel()
	p := &models.PeerInfo{}
	p.Address = s.internalAddr
	p.TotalBlocks = s.cold.NumBlocks()
	p.UUID = s.mds.UUID()
	err := s.mds.WithContext(ctx).RegisterPeer(p)
	if err != nil {
		clog.Warningf("couldn't heartbeat: %s", err)
	}
}
