package server

import (
	"net"
	"sync"

	"google.golang.org/grpc"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
)

type distributor struct {
	mut     sync.RWMutex
	blocks  agro.BlockStore
	inodes  agro.INodeStore
	srv     *server
	client  *distClient
	lis     net.Listener
	grpcSrv *grpc.Server

	ring   agro.Ring
	closed bool
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
	go d.rebalancer()
	d.client = newDistClient(d)
	return d, nil
}

func (d *distributor) Close() error {
	if d.closed {
		return nil
	}
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
