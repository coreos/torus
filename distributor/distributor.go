// distributor is a complex implementation of a Torus storage interface, that
// understands rebalancing it's underlying storage and fetching data from peers,
// as necessary.
package distributor

import (
	"net/url"
	"sync"

	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/torus"
	"github.com/coreos/torus/distributor/protocols"
	"github.com/coreos/torus/distributor/rebalance"
	"github.com/coreos/torus/gc"
)

var (
	clog = capnslog.NewPackageLogger("github.com/coreos/torus", "distributor")
)

type Distributor struct {
	mut       sync.RWMutex
	blocks    torus.BlockStore
	srv       *torus.Server
	client    *distClient
	rpcSrv    protocols.RPCServer
	readCache *cache

	ring            torus.Ring
	closed          bool
	rebalancerChan  chan struct{}
	ringWatcherChan chan struct{}
	rebalancer      rebalance.Rebalancer
	rebalancing     bool
}

func newDistributor(srv *torus.Server, addr *url.URL) (*Distributor, error) {
	var err error
	d := &Distributor{
		blocks: srv.Blocks,
		srv:    srv,
	}
	gmd := d.srv.MDS.GlobalMetadata()
	if addr != nil {
		d.rpcSrv, err = protocols.ListenRPC(addr, d, gmd)
		if err != nil {
			return nil, err
		}
	}
	if srv.Cfg.ReadCacheSize != 0 {
		size := srv.Cfg.ReadCacheSize / gmd.BlockSize
		if size < 100 {
			size = 100
		}
		d.readCache = newCache(int(size))
	}

	// Set up the rebalancer
	d.ring, err = d.srv.MDS.GetRing()
	if err != nil {
		return nil, err
	}
	d.ringWatcherChan = make(chan struct{})
	go d.ringWatcher(d.rebalancerChan)
	d.client = newDistClient(d)
	g := gc.NewGCController(d.srv, torus.NewINodeStore(d))
	d.rebalancer = rebalance.NewRebalancer(d, d.blocks, d.client, g)
	d.rebalancerChan = make(chan struct{})
	go d.rebalanceTicker(d.rebalancerChan)
	return d, nil
}

func (d *Distributor) UUID() string {
	return d.srv.MDS.UUID()
}

func (d *Distributor) Ring() torus.Ring {
	d.mut.RLock()
	defer d.mut.RUnlock()
	return d.ring
}

func (d *Distributor) Close() error {
	d.mut.Lock()
	defer d.mut.Unlock()
	if d.closed {
		return nil
	}
	close(d.rebalancerChan)
	close(d.ringWatcherChan)
	if d.rpcSrv != nil {
		d.rpcSrv.Close()
	}
	d.client.Close()
	err := d.blocks.Close()
	if err != nil {
		return err
	}
	d.closed = true
	return nil
}
