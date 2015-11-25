package blockset

import (
	"testing"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/storage/block"
)

type makeTestBlockset func(s agro.BlockStore) blockset

func TestBaseReadWrite(t *testing.T) {
	s := block.OpenTempBlockStore()
	b := newBaseBlockset(s)
	readWriteTest(t, b)
}

func readWriteTest(t *testing.T, b blockset) {
	inode := agro.INodeRef{1, 1}
	b.PutBlock(inode, 0, []byte("Some data"))
	inode = agro.INodeRef{1, 2}
	b.PutBlock(inode, 1, []byte("Some more data"))
	data, err := b.GetBlock(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "Some data" {
		t.Error("data not retrieved")
	}
	b.PutBlock(inode, 0, []byte("Some different data"))
	data, err = b.GetBlock(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "Some different data" {
		t.Error("data not retrieved")
	}
}

func TestBaseMarshal(t *testing.T) {
	s := block.OpenTempBlockStore()
	marshalTest(t, s, agro.BlockLayerSpec{Base})
}

func marshalTest(t *testing.T, s agro.BlockStore, spec agro.BlockLayerSpec) {
	b, err := CreateBlocksetFromSpec(spec, s)
	if err != nil {
		t.Fatal(err)
	}
	inode := agro.INodeRef{1, 1}
	b.PutBlock(inode, 0, []byte("Some data"))
	marshal, err := MarshalToProto(b)
	if err != nil {
		t.Fatal(err)
	}

	b = nil
	newb, err := UnmarshalFromProto(marshal, s)
	if err != nil {
		t.Fatal(err)
	}

	data, err := newb.GetBlock(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "Some data" {
		t.Error("data not retrieved")
	}
}
