package server

import "github.com/barakmich/agro"

func (s *server) ListenReplication(addr string) error {
	return s.openReplication(addr, true)
}

func (s *server) OpenReplication() error {
	return s.openReplication("", false)
}
func (s *server) openReplication(addr string, listen bool) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	if s.replicationOpen {
		return agro.ErrExists
	}
	s.internalAddr = addr
	dist, err := newDistributor(s, addr, listen)
	if err != nil {
		return err
	}
	s.blocks = dist
	s.inodes = dist
	err = s.BeginHeartbeat()
	if err != nil {
		return err
	}
	s.replicationOpen = true
	return nil
}
