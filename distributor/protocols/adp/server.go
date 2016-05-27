package adp

import (
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/coreos/torus"
	"github.com/coreos/torus/distributor/protocols"
)

const defaultPort = "40000"

func init() {
	protocols.RegisterRPCListener("adp", adpRPCListener)
	protocols.RegisterRPCDialer("adp", adpRPCDialer)
}

func adpRPCListener(url *url.URL, handler protocols.RPC, gmd torus.GlobalMetadata) (protocols.RPCServer, error) {
	if strings.Contains(url.Host, ":") {
		return Serve(url.Host, handler, gmd.BlockSize)
	}
	return Serve(net.JoinHostPort(url.Host, defaultPort), handler, gmd.BlockSize)
}

func adpRPCDialer(url *url.URL, timeout time.Duration, gmd torus.GlobalMetadata) (protocols.RPC, error) {
	if strings.Contains(url.Host, ":") {
		return Dial(url.Host, timeout, gmd.BlockSize)
	}
	return Dial(net.JoinHostPort(url.Host, defaultPort), timeout, gmd.BlockSize)
}
