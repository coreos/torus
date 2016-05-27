package distributor

import (
	"net/url"
	"strings"

	"github.com/coreos/torus"

	// Import all the protocols we understand
	_ "github.com/coreos/torus/distributor/protocols/adp"
	_ "github.com/coreos/torus/distributor/protocols/grpc"
)

// ListenReplication opens the internal networking port and connects to the cluster
func ListenReplication(s *torus.Server, addr *url.URL) error {
	return openReplication(s, addr)
}

// OpenReplication connects to the cluster without opening the internal networking.
func OpenReplication(s *torus.Server) error {
	return openReplication(s, nil)
}
func openReplication(s *torus.Server, addr *url.URL) error {
	var err error
	if s.ReplicationOpen {
		return torus.ErrExists
	}
	dist, err := newDistributor(s, addr)
	if err != nil {
		return err
	}
	s.Blocks = dist
	s.INodes = torus.NewINodeStore(dist)
	err = s.BeginHeartbeat(addr)
	if err != nil {
		return err
	}
	s.ReplicationOpen = true
	return nil
}

func addrToUri(addr string) (*url.URL, error) {
	if strings.Contains(addr, "://") {
		// Looks like a full uri
		return url.Parse(addr)
	}
	return url.Parse("http://" + addr)
}
