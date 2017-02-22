package block_device

import (
	"bytes"
	"testing"
	"time"
)

func TestMetadataMarshal(t *testing.T) {
	m := &Metadata{
		TorusBlockSize:     512 * 1024,
		FormattedTimestamp: time.Unix(0x0000000011111111, 0),
		UsedBlocks:         0x1111111122222222,
	}

	data := m.Marshal()

	if !bytes.Equal(data[:MagicLen], MagicSentence) {
		t.Errorf("marshalled data didn't have magic sentence")
	}
	// 0x0000000000080000 is 512*1024 in hex
	if !bytes.Equal(data[MagicLen:MagicLen+8], []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00}) {
		t.Errorf("block size wasn't expected value")
	}
	if !bytes.Equal(data[MagicLen+8:MagicLen+16], []byte{0x00, 0x00, 0x00, 0x00, 0x11, 0x11, 0x11, 0x11}) {
		t.Errorf("timestamp wasn't expected value")
	}
	if !bytes.Equal(data[MagicLen+16:MagicLen+24], []byte{0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x22, 0x22}) {
		t.Errorf("used blocks wasn't expected value")
	}
}

func TestMetadataUnmarshal(t *testing.T) {
	buf := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, // block size
		0x00, 0x00, 0x00, 0x00, 0x11, 0x11, 0x11, 0x11, // time stamp
		0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x22, 0x22, // used blocks
	}
	buf = append(MagicSentence, buf...)
	m := &Metadata{}
	err := m.Unmarshal(buf)
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	if m.TorusBlockSize != 512*1024 {
		t.Errorf("Unmarshalled data didn't have expected block size")
	}
	if m.FormattedTimestamp != time.Unix(0x0000000011111111, 0) {
		t.Errorf("Unmarshalled data didn't have expected timestamp")
	}
	if m.UsedBlocks != 0x1111111122222222 {
		t.Errorf("Unmarshalled data didn't have expected timestamp")
	}
}
