package blockset

import (
	"encoding/binary"
	"hash/crc32"
	"sync"

	"golang.org/x/net/context"

	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/torus"
)

type crcBlockset struct {
	sub      blockset
	crcs     []uint32
	mut      sync.RWMutex
	emptyCrc uint32
}

var _ blockset = &crcBlockset{}

func init() {
	RegisterBlockset(CRC, func(_ string, _ torus.BlockStore, sub blockset) (blockset, error) {
		return newCRCBlockset(sub), nil
	})
}

func newCRCBlockset(sub blockset) *crcBlockset {
	b := &crcBlockset{
		crcs: nil,
		sub:  sub,
	}
	return b
}

func (b *crcBlockset) Length() int {
	b.mut.RLock()
	defer b.mut.RUnlock()
	if b.sub.Length() != len(b.crcs) {
		panic("crcs should always be as long as the sub blockset")
	}
	return len(b.crcs)
}

func (b *crcBlockset) Kind() uint32 {
	return uint32(CRC)
}

func (b *crcBlockset) GetBlock(ctx context.Context, i int) ([]byte, error) {
	b.mut.RLock()
	defer b.mut.RUnlock()
	if i >= len(b.crcs) {
		clog.Trace("crc: requesting block off the edge of known blocks")
		return nil, torus.ErrBlockNotExist
	}
	data, err := b.sub.GetBlock(ctx, i)
	if err != nil {
		clog.Trace("crc: error requesting subblock")
		return nil, err
	}
	crc := crc32.ChecksumIEEE(data)
	if crc != b.crcs[i] {
		clog.Warningf("crc: block %d did not pass crc", i)
		clog.Debugf("crc: %x should be %x\ndata : %v\n\n", crc, b.crcs[i], data[:10])
		promCRCFail.Inc()
		return nil, torus.ErrBlockUnavailable
	}
	return data, nil
}

func (b *crcBlockset) PutBlock(ctx context.Context, inode torus.INodeRef, i int, data []byte) error {
	b.mut.Lock()
	defer b.mut.Unlock()
	if i > len(b.crcs) {
		return torus.ErrBlockNotExist
	}
	crc := crc32.ChecksumIEEE(data)
	if crc == b.emptyCrc {
		ctx = context.WithValue(ctx, "isEmpty", true)
	}
	err := b.sub.PutBlock(ctx, inode, i, data)
	if err != nil {
		return err
	}
	if i == len(b.crcs) {
		b.crcs = append(b.crcs, crc)
	} else {
		b.crcs[i] = crc
	}
	if clog.LevelAt(capnslog.TRACE) {
		clog.Tracef("crc: setting crc %x at index %d", crc, i)
	}
	return nil
}

func (b *crcBlockset) makeID(i torus.INodeRef) torus.BlockRef {
	return b.sub.makeID(i)
}

func (b *crcBlockset) setStore(s torus.BlockStore) {
	b.emptyCrc = crc32.ChecksumIEEE(make([]byte, s.BlockSize()))
	b.sub.setStore(s)
}

func (b *crcBlockset) getStore() torus.BlockStore {
	return b.sub.getStore()
}

func (b *crcBlockset) Marshal() ([]byte, error) {
	b.mut.RLock()
	defer b.mut.RUnlock()
	buf := make([]byte, (len(b.crcs))*4)
	order := binary.LittleEndian
	for i, x := range b.crcs {
		order.PutUint32(buf[(i*4):((i+1)*4)], x)
	}
	return buf, nil
}

func (b *crcBlockset) Unmarshal(data []byte) error {
	b.mut.Lock()
	defer b.mut.Unlock()
	l := len(data) / 4
	out := make([]uint32, l)
	order := binary.LittleEndian
	for i := 0; i < l; i++ {
		out[i] = order.Uint32(data[(i * 4) : (i+1)*4])
	}
	b.crcs = out
	return nil
}

func (b *crcBlockset) GetSubBlockset() torus.Blockset { return b.sub }

func (b *crcBlockset) GetLiveINodes() *roaring.Bitmap {
	b.mut.RLock()
	defer b.mut.RUnlock()
	return b.sub.GetLiveINodes()
}

func (b *crcBlockset) Truncate(lastIndex int, blocksize uint64) error {
	b.mut.Lock()
	defer b.mut.Unlock()
	err := b.sub.Truncate(lastIndex, blocksize)
	if err != nil {
		return err
	}
	if lastIndex <= len(b.crcs) {
		b.crcs = b.crcs[:lastIndex]
		return nil
	}
	crc := crc32.ChecksumIEEE(make([]byte, blocksize))
	toadd := lastIndex - len(b.crcs)
	for toadd != 0 {
		b.crcs = append(b.crcs, crc)
		toadd--
	}
	return nil
}

func (b *crcBlockset) Trim(from, to int) error {
	b.mut.Lock()
	defer b.mut.Unlock()
	err := b.sub.Trim(from, to)
	if err != nil {
		return err
	}
	if from >= len(b.crcs) {
		return nil
	}
	if to > len(b.crcs) {
		to = len(b.crcs)
	}
	b.emptyCrc = crc32.ChecksumIEEE(make([]byte, b.getStore().BlockSize()))
	for i := from; i < to; i++ {
		b.crcs[i] = b.emptyCrc
	}
	return nil
}

func (b *crcBlockset) GetAllBlockRefs() []torus.BlockRef {
	b.mut.Lock()
	defer b.mut.Unlock()
	return b.sub.GetAllBlockRefs()
}

func (b *crcBlockset) String() string {
	return "crc\n" + b.sub.String()
}
