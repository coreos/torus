package server

import (
	"net"
	"strings"

	"github.com/coreos/agro"
)

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
	ipaddr := strings.SplitN(addr, ":", 2)
	ipaddr[0] = autodetectIP(ipaddr[0])
	s.peerInfo.Address = strings.Join(ipaddr, ":")
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
