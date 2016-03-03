package server

import (
	"errors"
	"sync"
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	clientTimeout          = 100 * time.Millisecond
	connectTimeout         = 2 * time.Second
	rebalanceClientTimeout = 5 * time.Second
	writeClientTimeout     = 2000 * time.Millisecond
)

// TODO(barakmich): Clean up errors

type distClient struct {
	dist *distributor
	//TODO(barakmich): Better connection pooling
	openConns   map[string]*grpc.ClientConn
	openClients map[string]models.AgroStorageClient
	mut         sync.Mutex
}

func newDistClient(d *distributor) *distClient {

	client := &distClient{
		dist:        d,
		openConns:   make(map[string]*grpc.ClientConn),
		openClients: make(map[string]models.AgroStorageClient),
	}
	d.srv.addTimeoutCallback(client.onPeerTimeout)
	return client
}

func (d *distClient) onPeerTimeout(uuid string) {
	d.mut.Lock()
	defer d.mut.Unlock()
	conn, ok := d.openConns[uuid]
	if !ok {
		return
	}
	delete(d.openClients, uuid)
	err := conn.Close()
	if err != nil {
		clog.Errorf("peer timeout err on close: %s", err)
	}
	delete(d.openConns, uuid)

}
func (d *distClient) getConn(uuid string) models.AgroStorageClient {
	d.mut.Lock()
	defer d.mut.Unlock()
	if conn, ok := d.openClients[uuid]; ok {
		return conn
	}
	d.dist.srv.mut.RLock()
	pi := d.dist.srv.peersMap[uuid]
	d.dist.srv.mut.RUnlock()
	if pi == nil {
		// We know this UUID exists, we don't have an address for it, let's refresh now.
		d.dist.srv.updatePeerMap()
		d.dist.srv.mut.RLock()
		pi = d.dist.srv.peersMap[uuid]
		d.dist.srv.mut.RUnlock()
		if pi == nil {
			// Not much more we can try
			return nil
		}
	}
	if pi.TimedOut {
		return nil
	}

	conn, err := grpc.Dial(pi.Address, grpc.WithInsecure(), grpc.WithTimeout(connectTimeout))
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
	resp, err := conn.Block(ctx, &models.BlockRequest{
		BlockRef: b.ToProto(),
	})
	if err != nil {
		clog.Debug(err)
		return nil, agro.ErrBlockUnavailable
	}
	if !resp.Ok {
		return nil, agro.ErrBlockUnavailable
	}
	return resp.Data, nil
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
