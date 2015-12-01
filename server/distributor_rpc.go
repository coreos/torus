package server

import (
	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
	"golang.org/x/net/context"
)

func (d *distributor) Block(ctx context.Context, req *models.BlockRequest) (*models.BlockResponse, error) {
	var out *models.BlockResponse
	for _, b := range req.Blockrefs {
		ref := agro.BlockRef{
			INodeRef: agro.INodeRef{
				Volume: agro.VolumeID(b.Volume),
				INode:  agro.INodeID(b.INode),
			},
			Index: agro.IndexID(b.Block),
		}
		data, err := d.blocks.GetBlock(ctx, ref)
		if err != nil {
			clog.Warningf("remote asking for non-existent block")
			out.Blocks = append(out.Blocks, &models.Block{
				Ok: false,
			})
			continue
		}
		out.Blocks = append(out.Blocks, &models.Block{
			Ok:   true,
			Data: data,
		})
	}
	return out, nil
}

func (d *distributor) INode(ctx context.Context, req *models.INodeRequest) (*models.INodeResponse, error) {
	var out *models.INodeResponse
	for _, b := range req.Inoderefs {
		ref := agro.INodeRef{
			Volume: agro.VolumeID(b.Volume),
			INode:  agro.INodeID(b.INode),
		}
		in, err := d.inodes.GetINode(ctx, ref)
		if err != nil {
			clog.Warningf("remote asking for non-existent inode")
			out.INodes = append(out.INodes, &models.INode{})
			continue
		}
		out.INodes = append(out.INodes, in)
	}
	return out, nil
}

func (d *distributor) PutBlock(ctx context.Context, req *models.PutBlockRequest) (*models.PutResponse, error) {
	return nil, nil
}

func (d *distributor) PutINode(ctx context.Context, req *models.PutINodeRequest) (*models.PutResponse, error) {
	return nil, nil
}
