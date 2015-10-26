package storage

import (
	"io/ioutil"
	"os"
	"testing"
)

func BenchmarkWrites(b *testing.B) {
	f, err := ioutil.TempFile(os.TempDir(), "mfiletest")
	if err != nil {
		b.Fatal(err)
	}
	n := f.Name()
	f.Close()
	os.Remove(n)
	err = CreateMFile(n, 1<<23)
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(n)
	m, err := OpenMFile(n, 1024)
	defer m.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.WriteBlock(uint64(i%(1<<13)), []byte("some data"))
		m.Flush()
	}
}
