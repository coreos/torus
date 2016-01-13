package server

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"golang.org/x/net/context"
)

func (d *distributor) Block(ctx context.Context, req *models.BlockRequest) (*models.BlockResponse, error) {
	out := &models.BlockResponse{}
	for _, b := range req.BlockRefs {
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
	out := &models.INodeResponse{}
	for _, b := range req.INodeRefs {
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
	if len(req.Refs) != len(req.Blocks) {
		return &models.PutResponse{Err: "malformed request"}, nil
	}
	for i, b := range req.Refs {
		ref := agro.BlockRef{
			INodeRef: agro.INodeRef{
				Volume: agro.VolumeID(b.Volume),
				INode:  agro.INodeID(b.INode),
			},
			Index: agro.IndexID(b.Block),
		}
		peers, err := d.ring.GetBlockPeers(ref)
		if err != nil {
			return nil, err
		}
		ok := false
		for _, x := range peers {
			if x == d.srv.mds.UUID() {
				ok = true
				break
			}
		}
		if !ok {
			clog.Warningf("trying to write block that doesn't belong to me. aborting.")
			return &models.PutResponse{Err: "not my block"}, nil
		}
		d.blocks.WriteBlock(ctx, ref, req.Blocks[i])
	}
	return &models.PutResponse{
		Ok: true,
	}, nil
}

func (d *distributor) PutINode(ctx context.Context, req *models.PutINodeRequest) (*models.PutResponse, error) {
	if len(req.Refs) != len(req.INodes) {
		return &models.PutResponse{Err: "malformed request"}, nil
	}
	for i, b := range req.Refs {
		ref := agro.INodeRef{
			Volume: agro.VolumeID(b.Volume),
			INode:  agro.INodeID(b.INode),
		}
		peers, err := d.ring.GetINodePeers(ref)
		if err != nil {
			return nil, err
		}
		ok := false
		for _, x := range peers {
			if x == d.srv.mds.UUID() {
				ok = true
				break
			}
		}
		if !ok {
			clog.Warningf("trying to write inode that doesn't belong to me. aborting.")
			return &models.PutResponse{Err: "not my inode"}, nil
		}
		d.inodes.WriteINode(ctx, ref, req.INodes[i])
	}
	return &models.PutResponse{
		Ok: true,
	}, nil
}
