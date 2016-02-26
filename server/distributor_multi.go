package server

import (
	"github.com/coreos/agro"
	"golang.org/x/net/context"
)

func (d *distributor) GetBlocks(ctx context.Context, bs []agro.BlockRef) ([][]byte, error) {
	if len(bs) == 0 {
		return nil, nil
	}
	owners := make(map[string][]int)
	for i, b := range bs {
		pl, err := d.ring.GetPeers(b)
		if err != nil {
			return nil, err
		}
		for _, x := range pl.Peers[:pl.Replication] {
			v, _ := owners[x]
			owners[x] = append(v, i)
		}
	}
	best := 0
	bestPeer := ""
	for k, v := range owners {
		if len(v) > best {
			best = len(v)
			bestPeer = k
		}
	}
	getIndices := owners[bestPeer]
	remainIndices := make([]int, len(bs)-len(getIndices))
	getRefs := make([]agro.BlockRef, len(getIndices))
	remainRefs := make([]agro.BlockRef, len(bs)-len(getIndices))
	getIdx := 0
	remainIdx := 0
	for i := 0; i < len(bs); i++ {
		found := false
		for _, x := range getIndices {
			if x == i {
				getRefs[getIdx] = bs[i]
				getIdx++
				found = true
				break
			}
		}
		if found {
			continue
		}
		remainIndices[remainIdx] = i
		remainRefs[remainIdx] = bs[i]
		remainIdx++
	}
	blocks, err := d.client.GetBlocks(ctx, bestPeer, getRefs)
	if err != nil {
		return nil, err
	}
	remainBlocks, err := d.GetBlocks(ctx, remainRefs)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(bs))
	for i, x := range getIndices {
		out[x] = blocks[i]
	}
	for i, x := range remainIndices {
		out[x] = remainBlocks[i]
	}
	return out, nil
}

func (d *distributor) WriteBlocks(ctx context.Context, bs []agro.BlockRef, data [][]byte) error {
	panic("not implemented")
}
