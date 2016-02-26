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
	writeClientTimeout     = 2000 * time.Millisecond
	rebalanceClientTimeout = 5 * time.Second
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
	return &distClient{
		dist:        d,
		openConns:   make(map[string]*grpc.ClientConn),
		openClients: make(map[string]models.AgroStorageClient),
	}
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

func (d *distClient) GetBlocks(ctx context.Context, uuid string, ref agro.BlockRef, ahead []agro.BlockRef) ([]byte, error) {
	conn := d.getConn(uuid)
	if conn == nil {
		return nil, agro.ErrNoPeer
	}
	refs := make([]*models.BlockRef, len(ahead))
	for i, x := range ahead {
		refs[i] = x.ToProto()
	}
	resp, err := conn.Block(ctx, &models.BlockRequest{
		BlockRefs: []*models.BlockRef{ref.ToProto()},
		Peer:      d.dist.UUID(),
		Readahead: refs,
	})
	if err != nil {
		clog.Debug(err)
		return nil, agro.ErrBlockUnavailable
	}
	for _, b := range resp.Blocks {
		if !b.Ok {
			return nil, agro.ErrBlockUnavailable
		}
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

func (d *distClient) SendAhead(uuid string, blks []*models.BlockRef) {
	conn := d.getConn(uuid)
	if conn == nil {
		clog.Errorf("sendahead: %s", agro.ErrNoPeer)
		return
	}
	ctx, cancel := context.WithTimeout(context.TODO(), writeClientTimeout)
	defer cancel()
	req := &models.PutBlockRequest{}
	for _, x := range blks {
		ref := agro.BlockFromProto(x)
		ok, err := d.dist.blocks.HasBlock(ctx, ref)
		if err != nil {
			clog.Errorf("sendahead: hasblock err %s", err)
			return
		}
		if ok {
			req.Refs = append(req.Refs, x)
			blk, err := d.dist.blocks.GetBlock(ctx, ref)
			if err != nil {
				clog.Errorf("sendahead: getblock err %s", err)
				return
			}
			req.Blocks = append(req.Blocks, blk)
		}
	}
	_, err := conn.ReadAheadBlock(ctx, req)
	if err != nil {
		clog.Errorf("sendahead: rpc err %s", err)
	}
}
