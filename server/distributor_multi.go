package server

import (
	"github.com/coreos/agro"
	"golang.org/x/net/context"
)

func (d *distributor) GetBlocks(ctx context.Context, ref agro.BlockRef, bs []agro.BlockRef) ([]byte, error) {
	return d.getBlockReadAhead(ctx, ref, bs)
}

func (d *distributor) WriteBlocks(ctx context.Context, bs []agro.BlockRef, data [][]byte) error {
	panic("not implemented")
}
