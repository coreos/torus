// protocols is the metapackage for the RPC protocols for how Torus' storage
// layer communicates with other storage servers.
package protocols

import (
	"fmt"
	"net/url"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/torus"
)

type RPC interface {
	PutBlock(ctx context.Context, ref torus.BlockRef, data []byte) error
	Block(ctx context.Context, ref torus.BlockRef) ([]byte, error)
	RebalanceCheck(ctx context.Context, refs []torus.BlockRef) ([]bool, error)
	Close() error

	// This is a little bit of a hack to avoid more allocations.
	WriteBuf(ctx context.Context, ref torus.BlockRef) ([]byte, error)
}

type RPCServer interface {
	Close() error
}

type RPCDialerFunc func(*url.URL, time.Duration, torus.GlobalMetadata) (RPC, error)
type RPCListenerFunc func(*url.URL, RPC, torus.GlobalMetadata) (RPCServer, error)

var rpcDialers map[string]RPCDialerFunc
var rpcListeners map[string]RPCListenerFunc

func RegisterRPCListener(scheme string, newFunc RPCListenerFunc) {
	if rpcListeners == nil {
		rpcListeners = make(map[string]RPCListenerFunc)
	}

	if _, ok := rpcListeners[scheme]; ok {
		panic("torus: attempted to register RPC Listener with scheme " + scheme + " twice")
	}

	rpcListeners[scheme] = newFunc
}

func ListenRPC(url *url.URL, handler RPC, gmd torus.GlobalMetadata) (RPCServer, error) {
	if rpcListeners[url.Scheme] == nil {
		return nil, fmt.Errorf("Unknown ListenRPC protocol '%s'", url.Scheme)
	}

	return rpcListeners[url.Scheme](url, handler, gmd)
}

func RegisterRPCDialer(scheme string, newFunc RPCDialerFunc) {
	if rpcDialers == nil {
		rpcDialers = make(map[string]RPCDialerFunc)
	}

	if _, ok := rpcDialers[scheme]; ok {
		panic("torus: attempted to register RPC Listener with scheme " + scheme + " twice")
	}

	rpcDialers[scheme] = newFunc
}

func DialRPC(url *url.URL, timeout time.Duration, gmd torus.GlobalMetadata) (RPC, error) {
	if rpcDialers[url.Scheme] == nil {
		return nil, fmt.Errorf("Unknown DialRPC protocol '%s'", url.Scheme)
	}

	return rpcDialers[url.Scheme](url, timeout, gmd)
}
