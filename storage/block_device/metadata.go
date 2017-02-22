package block_device

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

type Metadata struct {
	TorusBlockSize     uint64
	FormattedTimestamp time.Time
	UsedBlocks         uint64
}

func (m *Metadata) Marshal() []byte {
	buf := make([]byte, MetadataSize)
	for i, b := range MagicSentence {
		buf[i] = b
	}
	binary.BigEndian.PutUint64(buf[MagicLen:MagicLen+8], m.TorusBlockSize)
	binary.BigEndian.PutUint64(buf[MagicLen+8:MagicLen+16], uint64(m.FormattedTimestamp.Unix()))
	binary.BigEndian.PutUint64(buf[MagicLen+16:MagicLen+24], m.UsedBlocks)

	return buf
}

func (m *Metadata) Unmarshal(buf []byte) error {
	if !bytes.Equal(MagicSentence, buf[:MagicLen]) {
		return fmt.Errorf("device has not been formatted for torus")
	}

	m.TorusBlockSize = binary.BigEndian.Uint64(buf[MagicLen : MagicLen+8])
	timeStampNum := binary.BigEndian.Uint64(buf[MagicLen+8 : MagicLen+16])
	m.UsedBlocks = binary.BigEndian.Uint64(buf[MagicLen+16 : MagicLen+24])

	m.FormattedTimestamp = time.Unix(int64(timeStampNum), 0)

	return nil
}
