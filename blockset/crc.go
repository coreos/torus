package blockset

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
	"github.com/tgruben/roaring"
)

type crcBlockset struct {
	sub  blockset
	crcs []uint32
}

var _ blockset = &crcBlockset{}

func init() {
	RegisterBlockset(CRC, func(_ string, _ agro.BlockStore, sub blockset) (blockset, error) {
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
	if b.sub.Length() != len(b.crcs) {
		panic("crcs should always be as long as the sub blockset")
	}
	return len(b.crcs)
}

func (b *crcBlockset) Kind() uint32 {
	return uint32(CRC)
}

func (b *crcBlockset) GetBlock(ctx context.Context, i int) ([]byte, error) {
	if i >= len(b.crcs) {
		clog.Trace("crc: requesting block off the edge of known blocks")
		return nil, agro.ErrBlockNotExist
	}
	data, err := b.sub.GetBlock(ctx, i)
	if err != nil {
		clog.Trace("crc: error requesting subblock")
		return nil, err
	}
	crc := crc32.ChecksumIEEE(data)
	if crc != b.crcs[i] {
		clog.Debug("crc: block did not pass crc")
		promCRCFail.Inc()
		return nil, agro.ErrBlockUnavailable
	}
	return data, nil
}

func (b *crcBlockset) PutBlock(ctx context.Context, inode agro.INodeRef, i int, data []byte) error {
	if i > len(b.crcs) {
		return agro.ErrBlockNotExist
	}
	err := b.sub.PutBlock(ctx, inode, i, data)
	if err != nil {
		return err
	}
	crc := crc32.ChecksumIEEE(data)
	if i == len(b.crcs) {
		b.crcs = append(b.crcs, crc)
	} else {
		b.crcs[i] = crc
	}
	return nil
}

func (b *crcBlockset) makeID(i agro.INodeRef) agro.BlockRef {
	return b.sub.makeID(i)
}

func (b *crcBlockset) setStore(s agro.BlockStore) {
	b.sub.setStore(s)
}

func (b *crcBlockset) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, int32(len(b.crcs)))
	if err != nil {
		return nil, err
	}
	for _, x := range b.crcs {
		err := binary.Write(buf, binary.LittleEndian, x)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (b *crcBlockset) Unmarshal(data []byte) error {
	r := bytes.NewReader(data)
	var l int32
	err := binary.Read(r, binary.LittleEndian, &l)
	if err != nil {
		return err
	}
	out := make([]uint32, l)
	err = binary.Read(r, binary.LittleEndian, &out)
	if err != nil {
		return err
	}
	b.crcs = out
	return nil
}

func (b *crcBlockset) GetSubBlockset() agro.Blockset { return b.sub }

func (b *crcBlockset) GetLiveINodes() *roaring.RoaringBitmap {
	return b.sub.GetLiveINodes()
}
