package server

import (
	"github.com/barakmich/agro/models"
	"golang.org/x/net/context"
)

func (d *distributor) Block(ctx context.Context, req *models.BlockRequest) (*models.BlockResponse, error) {
	return nil, nil
}

func (d *distributor) INode(ctx context.Context, req *models.INodeRequest) (*models.INodeResponse, error) {
	return nil, nil
}

func (d *distributor) PutBlock(ctx context.Context, req *models.PutBlockRequest) (*models.PutResponse, error) {
	return nil, nil
}

func (d *distributor) PutINode(ctx context.Context, req *models.PutINodeRequest) (*models.PutResponse, error) {
	return nil, nil
}
