package torus_test

import (
	"bytes"
	"crypto/rand"
	"io"
	"io/ioutil"
	"testing"

	"github.com/coreos/torus"
	"github.com/coreos/torus/blockset"
	"github.com/coreos/torus/models"

	_ "github.com/coreos/torus/metadata/temp"
	_ "github.com/coreos/torus/storage"
)

func makeTestData(size int) []byte {
	out := make([]byte, size)
	_, err := rand.Read(out)
	if err != nil {
		panic(err)
	}
	return out
}

func makeFile(name string, t *testing.T) (*torus.Server, *torus.File) {
	srv := torus.NewMemoryServer()
	vol := &models.Volume{
		Name: name,
		Id:   3,
		Type: "test",
	}
	globals, err := srv.MDS.GlobalMetadata()
	if err != nil {
		t.Fatal(err)
	}
	bs, err := blockset.CreateBlocksetFromSpec(globals.DefaultBlockSpec, srv.Blocks)
	if err != nil {
		t.Fatal(err)
	}
	inode := models.NewEmptyINode()
	inode.INode = 1
	inode.Volume = vol.Id
	inode.Blocks, err = torus.MarshalBlocksetToProto(bs)
	f, err := srv.CreateFile(vol, inode, bs)
	if err != nil {
		t.Fatal(err)
	}
	return srv, f
}

func newFile(name string, t *testing.T) *torus.File {
	_, f := makeFile(name, t)
	return f
}

func TestReadAt(t *testing.T) {
	f := newFile("TestReadAt", t)
	defer f.Close()

	const data = "hello, world\n"
	io.WriteString(f, data)

	b := make([]byte, 5)
	n, err := f.ReadAt(b, 7)
	if err != nil || n != len(b) {
		t.Fatalf("ReadAt 7: %d, %v", n, err)
	}
	if string(b) != "world" {
		t.Fatalf("ReadAt 7: have %q want %q", string(b), "world")
	}
	_, err = f.SyncAllWrites()
	if err != nil {
		t.Fatalf("error on sync: %v", err)
	}
	b = make([]byte, 5)
	n, err = f.ReadAt(b, 7)
	if err != nil || n != len(b) {
		t.Fatalf("ReadAt 7: %d, %v", n, err)
	}
	if string(b) != "world" {
		t.Fatalf("ReadAt 7: have %q want %q", string(b), "world")
	}
}

func TestReadAtOffset(t *testing.T) {
	f := newFile("TestReadAtOffset", t)
	defer f.Close()

	const data = "hello, world\n"
	io.WriteString(f, data)

	f.Seek(0, 0)
	b := make([]byte, 5)

	n, err := f.ReadAt(b, 7)
	if err != nil || n != len(b) {
		t.Fatalf("ReadAt 7: %d, %v", n, err)
	}
	if string(b) != "world" {
		t.Fatalf("ReadAt 7: have %q want %q", string(b), "world")
	}

	n, err = f.Read(b)
	if err != nil || n != len(b) {
		t.Fatalf("Read: %d, %v", n, err)
	}
	if string(b) != "hello" {
		t.Fatalf("Read: have %q want %q", string(b), "hello")
	}
}

func TestWriteAt(t *testing.T) {
	f := newFile("TestWriteAt", t)
	defer f.Close()

	const data = "hello, world\n"
	io.WriteString(f, data)

	n, err := f.WriteAt([]byte("WORLD"), 7)
	if err != nil || n != 5 {
		t.Fatalf("WriteAt 7: %d, %v", n, err)
	}
	f.Seek(0, 0)

	b, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll %v: %v", f, err)
	}
	if string(b) != "hello, WORLD\n" {
		t.Fatalf("after write: have %q want %q", string(b), "hello, WORLD\n")
	}
}

func TestWriteAtBulk(t *testing.T) {
	f := newFile("TestWriteAt", t)
	defer f.Close()

	const data = "hello, world\n"
	io.WriteString(f, data)

	_, err := f.SyncAllWrites()
	if err != nil {
		t.Fatalf("can't sync: %v", err)
	}

	// Odd-shape to stress block edges.
	big := makeTestData(549)
	n, err := f.WriteAt(big, 7)
	if err != nil || n != len(big) {
		t.Fatalf("WriteAt 7: %d, %v", n, err)
	}
	f.Seek(0, 0)

	b, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll %v: %v", f, err)
	}
	if string(b[:6]) != "hello," {
		t.Fatalf("after write: have %q want %q", string(b), "hello,")
	}
	if !bytes.Equal(b[7:], big) {
		t.Fatal("byte strings aren't equal")
	}
}
