package blockset

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/coreos/agro"

	// Register storage drivers.
	_ "github.com/coreos/agro/storage"
)

type makeTestBlockset func(s agro.BlockStore) blockset

func TestBaseReadWrite(t *testing.T) {
	s, _ := agro.CreateBlockStore("temp", "test", agro.Config{StorageSize: 300 * 1024}, agro.GlobalMetadata{BlockSize: 1024})
	b := newBaseBlockset(s)
	readWriteTest(t, b)
}

func readWriteTest(t *testing.T, b blockset) {
	inode := agro.NewINodeRef(1, 1)
	b.PutBlock(context.TODO(), inode, 0, []byte("Some data"))
	inode = agro.NewINodeRef(1, 2)
	b.PutBlock(context.TODO(), inode, 1, []byte("Some more data"))
	data, err := b.GetBlock(context.TODO(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "Some data" {
		t.Error("data not retrieved")
	}
	b.PutBlock(context.TODO(), inode, 0, []byte("Some different data"))
	data, err = b.GetBlock(context.TODO(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "Some different data" {
		t.Error("data not retrieved")
	}
}

func TestBaseMarshal(t *testing.T) {
	s, _ := agro.CreateBlockStore("temp", "test", agro.Config{StorageSize: 300 * 1024}, agro.GlobalMetadata{BlockSize: 1024})
	marshalTest(t, s, MustParseBlockLayerSpec("base"))
}

func marshalTest(t *testing.T, s agro.BlockStore, spec agro.BlockLayerSpec) {
	b, err := CreateBlocksetFromSpec(spec, s)
	if err != nil {
		t.Fatal(err)
	}
	inode := agro.NewINodeRef(1, 1)
	b.PutBlock(context.TODO(), inode, 0, []byte("Some data"))
	marshal, err := agro.MarshalBlocksetToProto(b)
	if err != nil {
		t.Fatal(err)
	}

	b = nil
	newb, err := UnmarshalFromProto(marshal, s)
	if err != nil {
		t.Fatal(err)
	}

	data, err := newb.GetBlock(context.TODO(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "Some data" {
		t.Error("data not retrieved")
	}
}
