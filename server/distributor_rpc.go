package server

import (
	"errors"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"golang.org/x/net/context"
)

func (d *distributor) Block(ctx context.Context, req *models.BlockRequest) (*models.BlockResponse, error) {
	promDistBlockRPCs.Inc()
	out := &models.BlockResponse{}
	fail := false
	for _, b := range req.BlockRefs {
		ref := agro.BlockFromProto(b)
		data, err := d.blocks.GetBlock(ctx, ref)
		if err != nil {
			clog.Warningf("remote asking for non-existent block")
			out.Blocks = append(out.Blocks, &models.Block{
				Ok: false,
			})
			fail = true
			continue
		}
		out.Blocks = append(out.Blocks, &models.Block{
			Ok:   true,
			Data: data,
		})
	}
	if fail {
		promDistBlockRPCFailures.Inc()
	}
	return out, nil
}

func (d *distributor) PutBlock(ctx context.Context, req *models.PutBlockRequest) (*models.PutResponse, error) {
	d.mut.RLock()
	defer d.mut.RUnlock()
	promDistPutBlockRPCs.Inc()
	if len(req.Refs) != len(req.Blocks) {
		return &models.PutResponse{Err: "malformed request"}, nil
	}
	for i, b := range req.Refs {
		ref := agro.BlockFromProto(b)
		peers, err := d.ring.GetPeers(ref)
		if err != nil {
			promDistPutBlockRPCFailures.Inc()
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
			promDistPutBlockRPCFailures.Inc()
			return &models.PutResponse{Err: "not my block"}, nil
		}
		d.blocks.WriteBlock(ctx, ref, req.Blocks[i])
	}
	return &models.PutResponse{
		Ok: true,
	}, nil
}

func (d *distributor) RebalanceMessage(ctx context.Context, req *models.RebalanceRequest) (*models.RebalanceResponse, error) {
	d.mut.RLock()
	defer d.mut.RUnlock()
	promDistRebalanceRPCs.Inc()
	if d.rebalancer == nil {
		promDistRebalanceRPCFailures.Inc()
		return nil, errors.New("not rebalancing")
	}
	resp, err := d.rebalancer.RebalanceMessage(ctx, req)
	if err != nil {
		promDistRebalanceRPCFailures.Inc()
	}
	return resp, err
}
