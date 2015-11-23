package storage

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func makeTempFilename() (string, error) {
	f, err := ioutil.TempFile("", "mfiletest")
	if err != nil {
		return "", err
	}
	n := f.Name()
	f.Close()
	os.Remove(n)
	return n, nil
}

func benchmarkWrites(b *testing.B, flush bool) {
	n, err := makeTempFilename()
	if err != nil {
		b.Fatal(err)
	}
	err = CreateMFile(n, 1<<23) // 8 MB
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(n)
	m, err := OpenMFile(n, 1024) // Block Size: 1 KB
	defer m.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Cycle through the all the blocks in the file.
		//   (file size / block size = 8192 blocks = 1<<13)
		m.WriteBlock(uint64(i%(1<<13)), []byte("some data and more"))
		if flush {
			m.Flush()
		}
	}
}

func BenchmarkWrites(b *testing.B) {
	benchmarkWrites(b, true)
}

func BenchmarkWritesNoFlush(b *testing.B) {
	benchmarkWrites(b, false)
}

func TestWrites(t *testing.T) {
	n, err := makeTempFilename()
	if err != nil {
		t.Fatal(err)
	}
	err = CreateMFile(n, 1<<23) // 8 MB
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(n)
	m, err := OpenMFile(n, 1024) // Block Size: 1 KB
	defer m.Close()

	data := []byte("some data")
	m.WriteBlock(2, data)
	m.Flush()
	b := m.GetBlock(2)
	if !reflect.DeepEqual(b[:len(data)], data) {
		t.Fatal("Got useless data back")
	}
}
