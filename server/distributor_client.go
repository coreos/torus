package server

import (
	"time"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	clientTimeout = 500 * time.Millisecond
)

type distClient struct {
	dist *distributor
	//TODO(barakmich): Connection pooling/keep-alive
}

func newDistClient(d *distributor) *distClient {
	return &distClient{
		dist: d,
	}
}

func (d *distClient) GetBlock(ctx context.Context, addr string, b agro.BlockRef) ([]byte, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
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

func (d *distClient) GetINode(ctx context.Context, addr string, b agro.INodeRef) (*models.INode, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
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
