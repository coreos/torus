package blockset

import (
	"testing"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/storage"
)

type makeTestBlockset func(s agro.BlockStore) blockset

func TestBaseReadWrite(t *testing.T) {
	s := storage.OpenTempBlockStore()
	b := newBasicBlockset(s)
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
	s := storage.OpenTempBlockStore()
	marshalTest(t, s, func(s agro.BlockStore) blockset {
		return newBasicBlockset(s)
	})
}

func marshalTest(t *testing.T, s agro.BlockStore, f makeTestBlockset) {
	b := f(s)
	inode := agro.INodeRef{1, 1}
	b.PutBlock(inode, 0, []byte("Some data"))
	var layerserial [][]byte
	var layer agro.Blockset
	for layer = b; layer != nil; layer = layer.GetSubBlockset() {
		m, err := layer.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		layerserial = append(layerserial, m)
	}
	b = nil
	newb := f(s)
	i := 0
	for layer = newb; layer != nil; layer = layer.GetSubBlockset() {
		err := layer.Unmarshal(layerserial[i])
		if err != nil {
			t.Fatal(err)
		}
		i++
	}

	data, err := newb.GetBlock(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "Some data" {
		t.Error("data not retrieved")
	}
}
