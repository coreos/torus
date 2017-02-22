package block_device

import (
	"bytes"
	"testing"
)

var (
	volume   uint64 = 0x0101010101010101
	inode    uint64 = 0x0202020202020202
	index    uint64 = 0x0303030303030303
	location uint64 = 0x0404040404040404

	volumeBytes   = []byte{0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}
	inodeBytes    = []byte{0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02}
	indexBytes    = []byte{0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03}
	locationBytes = []byte{0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04}
)

func TestBlockHeaderUnmarshal(t *testing.T) {
	var buf []byte
	buf = append(buf, volumeBytes...)
	buf = append(buf, inodeBytes...)
	buf = append(buf, indexBytes...)
	buf = append(buf, locationBytes...)

	b := BlockHeader{}
	b.Unmarshal(buf)

	expectedB := &BlockHeader{
		Volume:   volume,
		Inode:    inode,
		Index:    index,
		Location: location,
	}

	if !b.Equal(expectedB) {
		t.Errorf("block header unmarshalled incorrectly!\ndata: %s\nexpected: %s", b.String(), expectedB.String())
	}
}

func TestBlockHeaderMarshal(t *testing.T) {
	b := &BlockHeader{
		Volume:   volume,
		Inode:    inode,
		Index:    index,
		Location: location,
	}
	buf := make([]byte, BlockHeaderSize)
	b.Marshal(buf)

	var expectedBuf []byte
	expectedBuf = append(expectedBuf, volumeBytes...)
	expectedBuf = append(expectedBuf, inodeBytes...)
	expectedBuf = append(expectedBuf, indexBytes...)
	expectedBuf = append(expectedBuf, locationBytes...)

	if !bytes.Equal(buf, expectedBuf) {
		t.Errorf("block header marshalled incorrectly!\ndata: %v\nexpected: %v", buf, expectedBuf)
	}
}
