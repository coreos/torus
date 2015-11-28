package blockset

import (
	"testing"

	"github.com/barakmich/agro"

	// Register storage drivers.
	_ "github.com/barakmich/agro/storage/block"
)

func TestCRCReadWrite(t *testing.T) {
	s, _ := agro.CreateBlockStore("temp", agro.Config{}, agro.GlobalMetadata{})
	b := newBaseBlockset(s)
	crc := newCRCBlockset(b)
	readWriteTest(t, crc)
}

func TestCRCMarshal(t *testing.T) {
	s, _ := agro.CreateBlockStore("temp", agro.Config{}, agro.GlobalMetadata{})
	marshalTest(t, s, agro.BlockLayerSpec{CRC, Base})
}

func TestCRCCorruption(t *testing.T) {
	s, _ := agro.CreateBlockStore("temp", agro.Config{}, agro.GlobalMetadata{})
	b := newBaseBlockset(s)
	crc := newCRCBlockset(b)
	inode := agro.INodeRef{1, 1}
	crc.PutBlock(inode, 0, []byte("Some data"))
	s.WriteBlock(b.blocks[0], []byte("Evil Corruption!!"))
	_, err := crc.GetBlock(0)
	if err != agro.ErrBlockUnavailable {
		t.Fatal("No corruption detection")
	}
}
