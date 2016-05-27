package blockset

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/coreos/torus"

	// Register storage drivers.
	_ "github.com/coreos/torus/storage"
)

func TestCRCReadWrite(t *testing.T) {
	s, _ := torus.CreateBlockStore("temp", "test", torus.Config{StorageSize: 300 * 1024}, torus.GlobalMetadata{BlockSize: 1024})
	b := newBaseBlockset(s)
	crc := newCRCBlockset(b)
	readWriteTest(t, crc)
}

func TestCRCMarshal(t *testing.T) {
	s, _ := torus.CreateBlockStore("temp", "test", torus.Config{StorageSize: 300 * 1024}, torus.GlobalMetadata{BlockSize: 1024})
	marshalTest(t, s, MustParseBlockLayerSpec("crc,base"))
}

func TestCRCCorruption(t *testing.T) {
	s, _ := torus.CreateBlockStore("temp", "test", torus.Config{StorageSize: 300 * 1024}, torus.GlobalMetadata{BlockSize: 1024})
	b := newBaseBlockset(s)
	crc := newCRCBlockset(b)
	inode := torus.NewINodeRef(1, 1)
	crc.PutBlock(context.TODO(), inode, 0, []byte("Some data"))
	s.WriteBlock(context.TODO(), b.blocks[0], []byte("Evil Corruption!!"))
	_, err := crc.GetBlock(context.TODO(), 0)
	if err != torus.ErrBlockUnavailable {
		t.Fatal("No corruption detection")
	}
}
