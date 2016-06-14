package torus

import (
	"time"

	"golang.org/x/net/context"
)

type fileCache interface {
	newINode(ref INodeRef)
	writeToBlock(ctx context.Context, i, from, to int, data []byte) (int, error)
	getBlock(ctx context.Context, i int) ([]byte, error)
	sync(context.Context) error
}

type singleBlockCache struct {
	// half-finished blocks
	openIdx   int
	openData  []byte
	openWrote bool

	ref INodeRef

	blocks Blockset

	readIdx  int
	readData []byte

	blkSize uint64
}

func newSingleBlockCache(bs Blockset, blkSize uint64) *singleBlockCache {
	return &singleBlockCache{
		readIdx: -1,
		openIdx: -1,
		blocks:  bs,
		blkSize: blkSize,
	}
}

func (sb *singleBlockCache) newINode(ref INodeRef) {
	sb.ref = ref
}

func (sb *singleBlockCache) openBlock(ctx context.Context, i int) error {
	if sb.openIdx == i && sb.openData != nil {
		return nil
	}
	if sb.openWrote {
		err := sb.sync(ctx)
		if err != nil {
			return err
		}
	}
	if i == sb.blocks.Length() {
		sb.openData = make([]byte, sb.blkSize)
		sb.openIdx = i
		return nil
	}
	if i > sb.blocks.Length() {
		panic("writing beyond the end of a file without calling Truncate")
	}

	if sb.readIdx == i {
		sb.openIdx = i
		sb.openData = sb.readData
		sb.readData = nil
		sb.readIdx = -1
		return nil
	}
	start := time.Now()
	d, err := sb.blocks.GetBlock(ctx, i)
	if err != nil {
		return err
	}
	delta := time.Since(start)
	promFileBlockRead.Observe(float64(delta.Nanoseconds()) / 1000)
	sb.openData = d
	sb.openIdx = i
	return nil
}

func (sb *singleBlockCache) writeToBlock(ctx context.Context, i, from, to int, data []byte) (int, error) {
	if sb.openIdx != i {
		err := sb.openBlock(ctx, i)
		if err != nil {
			return 0, err
		}
	}
	sb.openWrote = true
	if (to - from) != len(data) {
		panic("server: different write lengths?")
	}
	return copy(sb.openData[from:to], data), nil
}

func (sb *singleBlockCache) sync(ctx context.Context) error {
	if !sb.openWrote {
		return nil
	}
	start := time.Now()
	err := sb.blocks.PutBlock(ctx, sb.ref, sb.openIdx, sb.openData)
	delta := time.Since(start)
	promFileBlockWrite.Observe(float64(delta.Nanoseconds()) / 1000)
	sb.openWrote = false
	return err
}

func (sb *singleBlockCache) openRead(ctx context.Context, i int) error {
	start := time.Now()
	d, err := sb.blocks.GetBlock(ctx, i)
	if err != nil {
		return err
	}
	delta := time.Since(start)
	promFileBlockRead.Observe(float64(delta.Nanoseconds()) / 1000)
	sb.readData = d
	sb.readIdx = i
	return nil
}

func (sb *singleBlockCache) getBlock(ctx context.Context, i int) ([]byte, error) {
	if sb.openIdx == i {
		return sb.openData, nil
	}
	if sb.readIdx != i {
		err := sb.openRead(ctx, i)
		if err != nil {
			return nil, err
		}
	}
	return sb.readData, nil
}
