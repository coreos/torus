package server

import (
	"errors"
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	clientTimeout      = 50 * time.Millisecond
	writeClientTimeout = 1000 * time.Millisecond
)

// TODO(barakmich): Clean up errors

type distClient struct {
	dist *distributor
	//TODO(barakmich): Better connection pooling
	openConns map[string]*grpc.ClientConn
}

func newDistClient(d *distributor) *distClient {
	return &distClient{
		dist:      d,
		openConns: make(map[string]*grpc.ClientConn),
	}
}

func (d *distClient) getConn(uuid string) *grpc.ClientConn {
	if conn, ok := d.openConns[uuid]; ok {
		return conn
	}
	pi := d.dist.srv.peersMap[uuid]
	if pi == nil {
		// We know this UUID exists, we don't have an address for it, let's refresh now.
		d.dist.srv.updatePeerMap()
		pi = d.dist.srv.peersMap[uuid]
		if pi == nil {
			// Not much more we can try
			return nil
		}
	}
	conn, err := grpc.Dial(pi.Address, grpc.WithInsecure())
	if err != nil {
		clog.Error(err)
		return nil
	}
	d.openConns[uuid] = conn
	return conn
}

func (d *distClient) Close() error {
	for _, c := range d.openConns {
		err := c.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *distClient) GetBlock(ctx context.Context, uuid string, b agro.BlockRef) ([]byte, error) {
	conn := d.getConn(uuid)
	if conn == nil {
		return nil, agro.ErrBlockUnavailable
	}
	client := models.NewAgroStorageClient(conn)
	newctx, cancel := context.WithTimeout(ctx, clientTimeout)
	defer cancel()
	resp, err := client.Block(newctx, &models.BlockRequest{
		BlockRefs: []*models.BlockRef{b.ToProto()},
	})
	if err != nil {
		clog.Debug(err)
		return nil, agro.ErrBlockUnavailable
	}
	if !resp.Blocks[0].Ok {
		return nil, agro.ErrBlockUnavailable
	}
	return resp.Blocks[0].Data, nil
}

func (d *distClient) PutBlock(ctx context.Context, uuid string, b agro.BlockRef, data []byte) error {
	conn := d.getConn(uuid)
	if conn == nil {
		return agro.ErrBlockUnavailable
	}
	client := models.NewAgroStorageClient(conn)
	newctx, cancel := context.WithTimeout(ctx, writeClientTimeout)
	defer cancel()
	resp, err := client.PutBlock(newctx, &models.PutBlockRequest{
		Refs:   []*models.BlockRef{b.ToProto()},
		Blocks: [][]byte{data},
	})
	if err != nil {
		if err == context.DeadlineExceeded {
			return agro.ErrBlockUnavailable
		}
		return err
	}
	if !resp.Ok {
		return errors.New(resp.Err)
	}
	return nil
}
