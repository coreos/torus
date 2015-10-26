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
		m.WriteBlock(uint64(i%(1<<13)), []byte("some data"))
		m.Flush()
	}
}
