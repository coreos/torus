package blockset

import (
	"testing"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/storage"
)

func TestCRCReadWrite(t *testing.T) {
	s := storage.OpenTempBlockStore()
	b := newBasicBlockset(s)
	crc := newCRCBlockset(b)
	readWriteTest(t, crc)
}

func TestCRCMarshal(t *testing.T) {
	s := storage.OpenTempBlockStore()
	marshalTest(t, s, func(s agro.BlockStore) blockset {
		b := newBasicBlockset(s)
		return newCRCBlockset(b)
	})
}

func TestCRCCorruption(t *testing.T) {
	s := storage.OpenTempBlockStore()
	b := newBasicBlockset(s)
	crc := newCRCBlockset(b)
	inode := agro.INodeRef{1, 1}
	crc.PutBlock(inode, 0, []byte("Some data"))
	s.WriteBlock(b.blocks[0], []byte("Evil Corruption!!"))
	_, err := crc.GetBlock(0)
	if err != agro.ErrBlockUnavailable {
		t.Fatal("No corruption detection")
	}
}
