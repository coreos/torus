package fs

import (
	"crypto/rand"
	"reflect"
	"testing"

	"github.com/coreos/agro"
)

func TestReadWrite(t *testing.T) {
	srv := agro.NewMemoryServer()
	err := srv.CreateFSVolume("test")
	if err != nil {
		t.Fatal(err)
	}
	testPath := Path{
		Volume: "test",
		Path:   "/foobar",
	}
	f, err := srv.Create(testPath)
	if err != nil {
		t.Fatal(err)
	}
	datalen := 2000
	data := make([]byte, datalen)
	rand.Read(data)
	written, err := f.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if written != datalen {
		t.Fatal("Didn't write it all")
	}
	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	f2, err := srv.Open(testPath)
	if err != nil {
		t.Fatal(err)
	}
	data2 := make([]byte, 192)
	n, err := f2.ReadAt(data2, 15)
	if err != nil {
		t.Fatal(err)
	}
	if n != 192 {
		t.Fatal("Didn't read enough")
	}
	if !reflect.DeepEqual(data2, data[15:15+192]) {
		t.Fatal("data didn't survive")
	}
}
