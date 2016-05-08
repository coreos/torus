package distributor

import (
	"github.com/coreos/agro"
	"golang.org/x/net/context"
)

func (d *Distributor) Block(ctx context.Context, ref agro.BlockRef) ([]byte, error) {
	promDistBlockRPCs.Inc()
	data, err := d.blocks.GetBlock(ctx, ref)
	if err != nil {
		promDistBlockRPCFailures.Inc()
		clog.Warningf("remote asking for non-existent block")
		return nil, agro.ErrBlockUnavailable
	}
	return data, nil
}

func (d *Distributor) PutBlock(ctx context.Context, ref agro.BlockRef, data []byte) error {
	d.mut.RLock()
	defer d.mut.RUnlock()
	promDistPutBlockRPCs.Inc()
	peers, err := d.ring.GetPeers(ref)
	if err != nil {
		promDistPutBlockRPCFailures.Inc()
		return err
	}
	ok := false
	for _, x := range peers.Peers {
		if x == d.UUID() {
			ok = true
			break
		}
	}
	if !ok {
		clog.Warningf("trying to write block that doesn't belong to me.")
	}
	err = d.blocks.WriteBlock(ctx, ref, data)
	if err != nil {
		return err
	}
	return d.Flush()
}

func (d *Distributor) RebalanceCheck(ctx context.Context, refs []agro.BlockRef) ([]bool, error) {
	out := make([]bool, len(refs))
	for i, x := range refs {
		ok, err := d.blocks.HasBlock(ctx, x)
		if err != nil {
			clog.Error(err)
			return nil, err
		}
		out[i] = ok
	}
	return out, nil
}
