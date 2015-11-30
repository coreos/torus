package server

import "github.com/barakmich/agro"

func (s *server) ListenReplication(addr string) error {
	if s.replicationOpen {
		return agro.ErrExists
	}
	return s.OpenReplication()
}

func (s *server) OpenReplication() error {
	if s.replicationOpen {
		return agro.ErrExists
	}
	s.replicationOpen = true
	return nil
}
