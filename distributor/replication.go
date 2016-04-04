package distributor

import (
	"fmt"
	"net"

	"github.com/coreos/agro"
)

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
	s.mut.Lock()
	defer s.mut.Unlock()
	if s.replicationOpen {
		return ErrExists
	}
	if addr != "" {
		ipaddr, port, err := net.SplitHostPort(addr)
		if err != nil {
			return err
		}
		ipaddr = autodetectIP(ipaddr)
		s.peerInfo.Address = fmt.Sprintf("%s:%s", ipaddr, port)
	}
	s.lease, err = s.mds.GetLease()
	if err != nil {
		return err
	}
	dist, err := newDistributor(s, addr, listen)
	if err != nil {
		return err
	}
	s.blocks = dist
	s.inodes = NewINodeStore(dist)
	err = s.BeginHeartbeat()
	if err != nil {
		return err
	}
	s.replicationOpen = true
	return nil
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
