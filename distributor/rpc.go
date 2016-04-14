package distributor

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"golang.org/x/net/context"
	"golang.org/x/net/trace"
)

func (d *Distributor) Block(ctx context.Context, req *models.BlockRequest) (*models.BlockResponse, error) {
	promDistBlockRPCs.Inc()
	t, tok := trace.FromContext(ctx)
	out := &models.BlockResponse{}
	fail := false
	ref := agro.BlockFromProto(req.BlockRef)
	if tok {
		t.LazyPrintf("got ref")
	}
	data, err := d.blocks.GetBlock(ctx, ref)
	if tok {
		t.LazyPrintf("got block")
	}
	if err != nil {
		clog.Warningf("remote asking for non-existent block")
		out.Ok = false
		fail = true
	} else {
		out.Ok = true
		out.Data = data
	}
	if fail {
		promDistBlockRPCFailures.Inc()
	}
	if tok {
		t.LazyPrintf("returning")
	}
	return out, nil
}

func (d *Distributor) PutBlock(ctx context.Context, req *models.PutBlockRequest) (*models.PutResponse, error) {
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
		for _, x := range peers.Peers {
			if x == d.UUID() {
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
	d.Flush()
	return &models.PutResponse{
		Ok: true,
	}, nil
}

func (d *Distributor) RebalanceCheck(ctx context.Context, req *models.RebalanceCheckRequest) (*models.RebalanceCheckResponse, error) {
	out := make([]bool, len(req.BlockRefs))
	for i, x := range req.BlockRefs {
		p := agro.BlockFromProto(x)
		ok, err := d.blocks.HasBlock(ctx, p)
		if err != nil {
			clog.Error(err)
		}
		out[i] = ok
	}
	return &models.RebalanceCheckResponse{
		Valid: out,
	}, nil
}
