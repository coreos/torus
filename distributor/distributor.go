package distributor

import (
	"net"
	"sync"

	"google.golang.org/grpc"

	"github.com/coreos/agro"
	"github.com/coreos/agro/distributor/rebalance"
	"github.com/coreos/agro/gc"
	"github.com/coreos/agro/models"
	"github.com/coreos/pkg/capnslog"
)

var (
	clog = capnslog.NewPackageLogger("github.com/coreos/agro", "distributor")
)

type Distributor struct {
	mut       sync.RWMutex
	blocks    agro.BlockStore
	srv       *agro.Server
	client    *distClient
	grpcSrv   *grpc.Server
	readCache *cache

	ring            agro.Ring
	closed          bool
	rebalancerChan  chan struct{}
	ringWatcherChan chan struct{}
	rebalancer      rebalance.Rebalancer
	rebalancing     bool
}

func newDistributor(srv *agro.Server, addr string, listen bool) (*Distributor, error) {
	var err error
	d := &Distributor{
		blocks: srv.Blocks,
		srv:    srv,
	}
	if listen {
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
		d.grpcSrv = grpc.NewServer()
		models.RegisterAgroStorageServer(d.grpcSrv, d)
		go d.grpcSrv.Serve(lis)
	}
	gmd, err := d.srv.MDS.GlobalMetadata()
	if err != nil {
		return nil, err
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
	g := gc.NewGCController(d.srv, agro.NewINodeStore(d))
	d.rebalancer = rebalance.NewRebalancer(d, d.blocks, d.client, g)
	d.rebalancerChan = make(chan struct{})
	go d.rebalanceTicker(d.rebalancerChan)
	return d, nil
}

func (d *Distributor) UUID() string {
	return d.srv.MDS.UUID()
}

func (d *Distributor) Ring() agro.Ring {
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
	if d.grpcSrv != nil {
		d.grpcSrv.Stop()
	}
	d.client.Close()
	err := d.blocks.Close()
	if err != nil {
		return err
	}
	d.closed = true
	return nil
}
