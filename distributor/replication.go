package distributor

import "github.com/coreos/agro"

// ListenReplication opens the internal networking port and connects to the cluster
func ListenReplication(s *agro.Server, addr string) error {
	return openReplication(s, addr, true)
}

// OpenReplication connects to the cluster without opening the internal networking.
func OpenReplication(s *agro.Server) error {
	return openReplication(s, "", false)
}
func openReplication(s *agro.Server, addr string, listen bool) error {
	var err error
	if s.ReplicationOpen {
		return agro.ErrExists
	}
	dist, err := newDistributor(s, addr, listen)
	if err != nil {
		return err
	}
	s.Blocks = dist
	s.INodes = agro.NewINodeStore(dist)
	err = s.BeginHeartbeat(addr, listen)
	if err != nil {
		return err
	}
	s.ReplicationOpen = true
	return nil
}
