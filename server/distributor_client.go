package server

import (
	"errors"
	"time"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	clientTimeout = 500 * time.Millisecond
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
	br := &models.BlockRef{
		Volume: uint64(b.Volume),
		INode:  uint64(b.INode),
		Block:  uint64(b.Index),
	}
	newctx, cancel := context.WithTimeout(ctx, clientTimeout)
	defer cancel()
	resp, err := client.Block(newctx, &models.BlockRequest{
		Blockrefs: []*models.BlockRef{br},
	})
	if err != nil {
		if err == context.DeadlineExceeded {
			return nil, agro.ErrBlockUnavailable
		}
		return nil, err
	}
	if !resp.Blocks[0].Ok {
		return nil, agro.ErrBlockUnavailable
	}
	return resp.Blocks[0].Data, nil
}

func (d *distClient) GetINode(ctx context.Context, uuid string, b agro.INodeRef) (*models.INode, error) {
	conn := d.getConn(uuid)
	if conn == nil {
		return nil, agro.ErrINodeUnavailable
	}
	client := models.NewAgroStorageClient(conn)
	ref := &models.INodeRef{
		Volume: uint64(b.Volume),
		INode:  uint64(b.INode),
	}
	newctx, cancel := context.WithTimeout(ctx, clientTimeout)
	defer cancel()
	resp, err := client.INode(newctx, &models.INodeRequest{
		Inoderefs: []*models.INodeRef{ref},
	})
	if err != nil {
		if err == context.DeadlineExceeded {
			return nil, agro.ErrINodeUnavailable
		}
		return nil, err
	}
	if resp.INodes[0].INode != uint64(b.INode) {
		return nil, agro.ErrINodeUnavailable
	}
	return resp.INodes[0], nil
}

func (d *distClient) PutINode(ctx context.Context, uuid string, b agro.INodeRef, inode *models.INode, rep int) error {
	conn := d.getConn(uuid)
	if conn == nil {
		return agro.ErrINodeUnavailable
	}
	client := models.NewAgroStorageClient(conn)
	ref := &models.INodeRef{
		Volume: uint64(b.Volume),
		INode:  uint64(b.INode),
	}
	newctx, cancel := context.WithTimeout(ctx, clientTimeout)
	defer cancel()
	resp, err := client.PutINode(newctx, &models.PutINodeRequest{
		Refs:              []*models.INodeRef{ref},
		INodes:            []*models.INode{inode},
		ReplicationFactor: uint32(rep),
	})
	if err != nil {
		if err == context.DeadlineExceeded {
			return agro.ErrINodeUnavailable
		}
		return err
	}
	if !resp.Ok {
		return errors.New(resp.Err)
	}
	return nil
}

func (d *distClient) PutBlock(ctx context.Context, uuid string, b agro.BlockRef, data []byte, rep int) error {
	conn := d.getConn(uuid)
	if conn == nil {
		return agro.ErrBlockUnavailable
	}
	client := models.NewAgroStorageClient(conn)
	ref := &models.BlockRef{
		Volume: uint64(b.Volume),
		INode:  uint64(b.INode),
		Block:  uint64(b.Index),
	}
	newctx, cancel := context.WithTimeout(ctx, clientTimeout)
	defer cancel()
	resp, err := client.PutBlock(newctx, &models.PutBlockRequest{
		Refs:              []*models.BlockRef{ref},
		Blocks:            [][]byte{data},
		ReplicationFactor: uint32(rep),
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
