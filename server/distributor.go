package server

import (
	"net"
	"sync"

	"google.golang.org/grpc"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
)

const (
	defaultInodeReplication = 3
)

type distributor struct {
	mut     sync.RWMutex
	blocks  agro.BlockStore
	inodes  agro.INodeStore
	srv     *server
	client  *distClient
	lis     net.Listener
	grpcSrv *grpc.Server

	ring           agro.Ring
	closed         bool
	rebalancerChan chan struct{}
}

func newDistributor(srv *server, addr string, listen bool) (*distributor, error) {
	var err error
	d := &distributor{
		blocks: srv.blocks,
		inodes: srv.inodes,
		srv:    srv,
	}
	if listen {
		d.lis, err = net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
		d.grpcSrv = grpc.NewServer()
		models.RegisterAgroStorageServer(d.grpcSrv, d)
		go d.grpcSrv.Serve(d.lis)
		srv.BeginHeartbeat()
	}
	// Set up the rebalancer
	d.ring, err = d.srv.mds.GetRing()
	if err != nil {
		return nil, err
	}
	d.rebalancerChan = make(chan struct{})
	go d.rebalancer(d.rebalancerChan)
	d.client = newDistClient(d)
	return d, nil
}

func (d *distributor) UUID() string {
	return d.srv.mds.UUID()
}

func (d *distributor) inodeReplication() int {
	r, err := d.srv.mds.GlobalMetadata()
	if err != nil {
		clog.Error(err)
		return defaultInodeReplication
	}
	return r.INodeReplication
}

func (d *distributor) Close() error {
	if d.closed {
		return nil
	}
	close(d.rebalancerChan)
	if d.lis != nil {
		d.grpcSrv.Stop()
		err := d.lis.Close()
		if err != nil {
			return err
		}
	}
	err := d.inodes.Close()
	if err != nil {
		return err
	}
	err = d.blocks.Close()
	if err != nil {
		return err
	}
	return nil
}
