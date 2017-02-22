package block_device

import (
	"encoding/binary"
	"fmt"

	"github.com/coreos/torus"
)

type BlockHeader struct {
	Volume   uint64
	Inode    uint64
	Index    uint64
	Location uint64
}

const (
	BlockHeaderSize = 32 // 4 fields * 8 byte fields
)

func (b *BlockHeader) Unmarshal(buf []byte) {
	b.Volume = binary.BigEndian.Uint64(buf[0:8])
	b.Inode = binary.BigEndian.Uint64(buf[8:16])
	b.Index = binary.BigEndian.Uint64(buf[16:24])
	b.Location = binary.BigEndian.Uint64(buf[24:32])
}

func (b *BlockHeader) Marshal(buf []byte) {
	binary.BigEndian.PutUint64(buf[0:8], b.Volume)
	binary.BigEndian.PutUint64(buf[8:16], b.Inode)
	binary.BigEndian.PutUint64(buf[16:24], b.Index)
	binary.BigEndian.PutUint64(buf[24:32], b.Location)
}

func (b *BlockHeader) IsNil() bool {
	return b.Volume == 0 && b.Inode == 0 && b.Index == 0
}

func (b *BlockHeader) String() string {
	return fmt.Sprintf("block header %d:%d:%d:%d", b.Volume, b.Inode, b.Index, b.Location)
}

func (b *BlockHeader) Equal(b2 *BlockHeader) bool {
	return b.Volume == b2.Volume && b.Inode == b2.Inode && b.Index == b2.Index && b.Location == b2.Location
}

type BlockHeaders []*BlockHeader

func NewBlockHeaders() BlockHeaders {
	bs := make(BlockHeaders, BlockSize/BlockHeaderSize)
	for i := 0; i < len(bs); i++ {
		bs[i] = &BlockHeader{}
	}
	return bs
}

func (bs BlockHeaders) Unmarshal(buf []byte) error {
	maxNumHeaders := len(buf) / BlockHeaderSize
	if maxNumHeaders > len(bs) {
		return fmt.Errorf("BlockHeaders.Unmarshal: buffer too large")
	}
	for i := 0; i < maxNumHeaders; i++ {
		n := i * BlockHeaderSize // current offset into buf
		bs[i].Unmarshal(buf[n : n+32])
	}
	return nil
}

func (bs BlockHeaders) Marshal(buf []byte) error {
	if len(bs)*BlockHeaderSize > len(buf) {
		return fmt.Errorf("BlockHeaders.Marshal: buffer too small")
	}
	for i := 0; i < len(bs); i++ {
		n := i * BlockHeaderSize // current offset into buf
		bs[i].Marshal(buf[n : n+32])
	}
	return nil
}

func (bs BlockHeaders) String() string {
	ret := ""
	for i, b := range bs {
		if b.IsNil() {
			break
		}
		if i != 0 {
			ret += "--------------------\n"
		}
		ret += fmt.Sprintf(" - volume:   %d\n", b.Volume)
		ret += fmt.Sprintf(" - inode:    %d\n", b.Inode)
		ret += fmt.Sprintf(" - index:    %d\n", b.Index)
		ret += fmt.Sprintf(" - location: %d\n", b.Location)
	}
	return ret
}

func (bs BlockHeaders) IsUsed() bool {
	blockUsed := false
	for _, b := range bs {
		if b.Location == 0 && !b.IsNil() {
			blockUsed = true
			break
		}
	}
	return blockUsed
}

func (bs BlockHeaders) Add(b *BlockHeader) error {
	for i := 0; i < len(bs); i++ {
		if bs[i].IsNil() {
			bs[i] = b
			return nil
		}
	}
	return fmt.Errorf("block header is full: %q", b.String())
}

func (bs BlockHeaders) Delete(br torus.BlockRef) {
	for i := 0; i < len(bs); i++ {
		b := bs[i]
		if b.Volume == uint64(br.Volume()) && b.Inode == uint64(br.INode) && b.Index == uint64(br.Index) {
			bs[i] = &BlockHeader{}
		}
	}
}
