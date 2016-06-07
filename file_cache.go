package torus

import (
	"time"

	"golang.org/x/net/context"
)

type fileCache struct {
	// half-finished blocks
	openIdx   int
	openData  []byte
	openWrote bool

	ref INodeRef

	blocks Blockset

	readData []byte
	readIdx  int
	blkSize  uint64
}

func newFileCache(bs Blockset, blkSize uint64) *fileCache {
	return &fileCache{
		readIdx: -1,
		openIdx: -1,
		blocks:  bs,
		blkSize: blkSize,
	}
}

func (fc *fileCache) newINode(ref INodeRef) {
	fc.ref = ref
}

func (fc *fileCache) openBlock(ctx context.Context, i int) error {
	if fc.openIdx == i && fc.openData != nil {
		return nil
	}
	if fc.openData != nil {
		err := fc.sync(ctx)
		if err != nil {
			return err
		}
	}
	if fc.blocks.Length() == i {
		fc.openData = make([]byte, fc.blkSize)
		fc.openIdx = i
		return nil
	}
	start := time.Now()
	d, err := fc.blocks.GetBlock(ctx, i)
	if err != nil {
		return err
	}
	delta := time.Now().Sub(start)
	promFileBlockRead.Observe(float64(delta.Nanoseconds()) / 1000)
	fc.openData = d
	fc.openIdx = i
	return nil
}

func (fc *fileCache) writeToBlock(ctx context.Context, i, from, to int, data []byte) (int, error) {
	fc.openWrote = true
	if fc.openIdx != i {
		err := fc.openBlock(ctx, i)
		if err != nil {
			return 0, err
		}
	}
	if (to - from) != len(data) {
		panic("server: different write lengths?")
	}
	return copy(fc.openData[from:to], data), nil
}

func (fc *fileCache) sync(ctx context.Context) error {
	if !fc.openWrote {
		return nil
	}
	start := time.Now()
	err := fc.blocks.PutBlock(ctx, fc.ref, fc.openIdx, fc.openData)
	delta := time.Now().Sub(start)
	promFileBlockWrite.Observe(float64(delta.Nanoseconds()) / 1000)
	fc.openIdx = -1
	fc.openData = nil
	fc.openWrote = false
	fc.readIdx = -1
	fc.readData = nil
	return err
}

func (fc *fileCache) openRead(ctx context.Context, i int) error {
	start := time.Now()
	d, err := fc.blocks.GetBlock(ctx, i)
	if err != nil {
		return err
	}
	delta := time.Now().Sub(start)
	promFileBlockRead.Observe(float64(delta.Nanoseconds()) / 1000)
	fc.readData = d
	fc.readIdx = i
	return nil
}

func (fc *fileCache) getBlock(ctx context.Context, i int) ([]byte, error) {
	if fc.openIdx == i {
		return fc.openData, nil
	}
	if fc.readIdx != i {
		err := fc.openRead(ctx, i)
		if err != nil {
			return nil, err
		}
	}
	return fc.readData, nil
}
