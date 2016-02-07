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
	clientTimeout          = 100 * time.Millisecond
	writeClientTimeout     = 2000 * time.Millisecond
	rebalanceClientTimeout = 5 * time.Second
)

// TODO(barakmich): Clean up errors

type distClient struct {
	dist *distributor
	//TODO(barakmich): Better connection pooling
	openConns   map[string]*grpc.ClientConn
	openClients map[string]models.AgroStorageClient
}

func newDistClient(d *distributor) *distClient {
	return &distClient{
		dist:        d,
		openConns:   make(map[string]*grpc.ClientConn),
		openClients: make(map[string]models.AgroStorageClient),
	}
}

func (d *distClient) getConn(uuid string) models.AgroStorageClient {
	if conn, ok := d.openClients[uuid]; ok {
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
		clog.Errorf("couldn't dial: %v", err)
		return nil
	}
	m := models.NewAgroStorageClient(conn)
	d.openConns[uuid] = conn
	d.openClients[uuid] = m
	return m
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
		return nil, agro.ErrNoPeer
	}
	newctx, cancel := context.WithTimeout(ctx, clientTimeout)
	defer cancel()
	resp, err := conn.Block(newctx, &models.BlockRequest{
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
	newctx, cancel := context.WithTimeout(ctx, writeClientTimeout)
	defer cancel()
	resp, err := conn.PutBlock(newctx, &models.PutBlockRequest{
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

func (d *distClient) Check(ctx context.Context, uuid string, blks []agro.BlockRef) ([]bool, error) {
	conn := d.getConn(uuid)
	if conn == nil {
		return nil, agro.ErrNoPeer
	}
	refs := make([]*models.BlockRef, 0, len(blks))
	for _, x := range blks {
		refs = append(refs, x.ToProto())
	}
	newctx, cancel := context.WithTimeout(ctx, rebalanceClientTimeout)
	defer cancel()
	resp, err := conn.RebalanceCheck(newctx, &models.RebalanceCheckRequest{
		BlockRefs: refs,
	})
	if err != nil {
		return nil, err
	}
	return resp.Valid, nil
}
