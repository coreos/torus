package blockset

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/coreos/agro"

	// Register storage drivers.
	_ "github.com/coreos/agro/storage/block"
)

func TestCRCReadWrite(t *testing.T) {
	s, _ := agro.CreateBlockStore("temp", "test", agro.Config{StorageSize: 300 * 1024}, agro.GlobalMetadata{BlockSize: 1024})
	b := newBaseBlockset(s)
	crc := newCRCBlockset(b)
	readWriteTest(t, crc)
}

func TestCRCMarshal(t *testing.T) {
	s, _ := agro.CreateBlockStore("temp", "test", agro.Config{StorageSize: 300 * 1024}, agro.GlobalMetadata{BlockSize: 1024})
	marshalTest(t, s, MustParseBlockLayerSpec("crc,base"))
}

func TestCRCCorruption(t *testing.T) {
	s, _ := agro.CreateBlockStore("temp", "test", agro.Config{StorageSize: 300 * 1024}, agro.GlobalMetadata{BlockSize: 1024})
	b := newBaseBlockset(s)
	crc := newCRCBlockset(b)
	inode := agro.NewINodeRef(1, 1)
	crc.PutBlock(context.TODO(), inode, 0, []byte("Some data"))
	s.WriteBlock(context.TODO(), b.blocks[0], []byte("Evil Corruption!!"))
	_, err := crc.GetBlock(context.TODO(), 0)
	if err != agro.ErrBlockUnavailable {
		t.Fatal("No corruption detection")
	}
}
