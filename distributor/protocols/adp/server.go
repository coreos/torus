package adp

import (
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/distributor"
)

const defaultPort = "40000"

func init() {
	distributor.RegisterRPCListener("adp", adpRPCListener)
	distributor.RegisterRPCDialer("adp", adpRPCDialer)
}

func adpRPCListener(url *url.URL, handler distributor.RPC, gmd agro.GlobalMetadata) (distributor.RPCServer, error) {
	if strings.Contains(url.Host, ":") {
		return Serve(url.Host, handler, gmd.BlockSize)
	}
	return Serve(net.JoinHostPort(url.Host, defaultPort), handler, gmd.BlockSize)
}

func adpRPCDialer(url *url.URL, timeout time.Duration, gmd agro.GlobalMetadata) (distributor.RPC, error) {
	if strings.Contains(url.Host, ":") {
		return Dial(url.Host, timeout, gmd.BlockSize)
	}
	return Dial(net.JoinHostPort(url.Host, defaultPort), timeout, gmd.BlockSize)
}
