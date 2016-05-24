package protocols

import (
	"net/url"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
)

type RPC interface {
	PutBlock(ctx context.Context, ref agro.BlockRef, data []byte) error
	Block(ctx context.Context, ref agro.BlockRef) ([]byte, error)
	RebalanceCheck(ctx context.Context, refs []agro.BlockRef) ([]bool, error)
	Close() error

	// This is a little bit of a hack to avoid more allocations.
	WriteBuf(ctx context.Context, ref agro.BlockRef) ([]byte, error)
}

type RPCServer interface {
	Close() error
}

type RPCDialerFunc func(*url.URL, time.Duration, agro.GlobalMetadata) (RPC, error)
type RPCListenerFunc func(*url.URL, RPC, agro.GlobalMetadata) (RPCServer, error)

var rpcDialers map[string]RPCDialerFunc
var rpcListeners map[string]RPCListenerFunc

func RegisterRPCListener(scheme string, newFunc RPCListenerFunc) {
	if rpcListeners == nil {
		rpcListeners = make(map[string]RPCListenerFunc)
	}

	if _, ok := rpcListeners[scheme]; ok {
		panic("agro: attempted to register RPC Listener with scheme " + scheme + " twice")
	}

	rpcListeners[scheme] = newFunc
}

func ListenRPC(url *url.URL, handler RPC, gmd agro.GlobalMetadata) (RPCServer, error) {
	return rpcListeners[url.Scheme](url, handler, gmd)
}

func RegisterRPCDialer(scheme string, newFunc RPCDialerFunc) {
	if rpcDialers == nil {
		rpcDialers = make(map[string]RPCDialerFunc)
	}

	if _, ok := rpcDialers[scheme]; ok {
		panic("agro: attempted to register RPC Listener with scheme " + scheme + " twice")
	}

	rpcDialers[scheme] = newFunc
}

func DialRPC(url *url.URL, timeout time.Duration, gmd agro.GlobalMetadata) (RPC, error) {
	return rpcDialers[url.Scheme](url, timeout, gmd)
}
