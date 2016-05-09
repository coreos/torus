package agro

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/block"
	"github.com/coreos/agro/distributor"
	"github.com/coreos/agro/metadata/temp"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"
	_ "github.com/coreos/agro/storage/block"
)

const (
	StorageSize = 100 * 1024 * 1024
	BlockSize   = 256
)

func newServer(md *temp.Server) *agro.Server {
	cfg := agro.Config{
		StorageSize: StorageSize,
	}
	mds := temp.NewClient(cfg, md)
	gmd, _ := mds.GlobalMetadata()
	blocks, _ := agro.CreateBlockStore("temp", "current", cfg, gmd)
	s, _ := agro.NewServerByImpl(cfg, mds, blocks)
	return s
}

func createN(t testing.TB, n int) ([]*agro.Server, *temp.Server) {
	var out []*agro.Server
	s := temp.NewServer()
	for i := 0; i < n; i++ {
		srv := newServer(s)
		err := distributor.ListenReplication(srv, fmt.Sprintf("127.0.0.1:%d", 40000+i))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, srv)
	}
	// Heartbeat
	time.Sleep(10 * time.Millisecond)
	return out, s
}

func ringN(t testing.TB, n int) ([]*agro.Server, *temp.Server) {
	servers, mds := createN(t, n)
	var peers agro.PeerInfoList
	for _, s := range servers {
		peers = append(peers, &models.PeerInfo{
			UUID:        s.MDS.UUID(),
			TotalBlocks: StorageSize / BlockSize,
		})
	}

	rep := 2
	ringType := ring.Ketama
	if n == 1 {
		rep = 1
		ringType = ring.Single
	}

	newRing, err := ring.CreateRing(&models.Ring{
		Type:              uint32(ringType),
		Peers:             peers,
		ReplicationFactor: uint32(rep),
		Version:           uint32(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = mds.SetRing(newRing)
	if err != nil {
		t.Fatal(err)
	}
	return servers, mds
}

func closeAll(t testing.TB, c ...*agro.Server) {
	for _, x := range c {
		err := x.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func makeTestData(size int) []byte {
	out := make([]byte, size)
	_, err := rand.Read(out)
	if err != nil {
		panic(err)
	}
	return out
}

func TestLoadAndDump(t *testing.T) {
	servers, mds := ringN(t, 3)
	client := newServer(mds)
	err := distributor.OpenReplication(client)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	size := BlockSize * 100
	data := makeTestData(size)
	input := bytes.NewReader(data)
	volname := "testvol"
	err = block.CreateBlockVolume(client.MDS, volname, uint64(size))
	if err != nil {
		t.Fatalf("couldn't create block volume %s: %v", volname, err)
	}
	blockvol, err := block.OpenBlockVolume(client, volname)
	if err != nil {
		t.Fatalf("couldn't open block volume %s: %v", volname, err)
	}
	f, err := blockvol.OpenBlockFile()
	if err != nil {
		t.Fatalf("couldn't open blockfile %s: %v", volname, err)
	}
	copied, err := io.Copy(f, input)
	if err != nil {
		t.Fatalf("couldn't copy: %v", err)
	}
	err = f.Sync()
	if err != nil {
		t.Fatalf("couldn't sync: %v", err)
	}
	err = f.Close()
	if err != nil {
		t.Fatalf("couldn't close: %v", err)
	}
	fmt.Printf("copied %d bytes\n", copied)

	for _, x := range servers {
		it := x.Blocks.BlockIterator()
		for {
			ok := it.Next()
			if !ok {
				err := it.Err()
				if err != nil {
					t.Fatal(err)
				}
				break
			}
			ref := it.BlockRef()
			t.Log(ref)
		}
	}

	reader := newServer(mds)
	err = distributor.OpenReplication(reader)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	blockvol, err = block.OpenBlockVolume(reader, volname)
	if err != nil {
		t.Fatalf("couldn't open block volume %s: %v", volname, err)
	}
	f, err = blockvol.OpenBlockFile()
	if err != nil {
		t.Fatalf("couldn't open blockfile %s: %v", volname, err)
	}
	output := &bytes.Buffer{}
	copied, err = io.Copy(output, f)
	if err != nil {
		t.Fatalf("couldn't copy: %v", err)
	}
	if !bytes.Equal(output.Bytes(), data) {
		t.Error("bytes not equal")
	}
	closeAll(t, servers...)
}

func BenchmarkLoadOne(b *testing.B) {
	b.StopTimer()

	servers, mds := ringN(b, 1)
	client := newServer(mds)
	err := distributor.OpenReplication(client)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	size := BlockSize * 1024
	data := makeTestData(size)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		input := bytes.NewReader(data)
		volname := fmt.Sprintf("testvol-%d")

		err = block.CreateBlockVolume(client.MDS, volname, uint64(size))
		if err != nil {
			b.Fatalf("couldn't create block volume %s: %v", volname, err)
		}
		blockvol, err := block.OpenBlockVolume(client, volname)
		if err != nil {
			b.Fatalf("couldn't open block volume %s: %v", volname, err)
		}
		f, err := blockvol.OpenBlockFile()
		if err != nil {
			b.Fatalf("couldn't open blockfile %s: %v", volname, err)
		}
		copied, err := io.Copy(f, input)
		if err != nil {
			b.Fatalf("couldn't copy: %v", err)
		}
		err = f.Sync()
		if err != nil {
			b.Fatalf("couldn't sync: %v", err)
		}
		err = f.Close()
		if err != nil {
			b.Fatalf("couldn't close: %v", err)
		}

		b.Logf("copied %d bytes", copied)
	}

	b.SetBytes(int64(size))

	b.StopTimer()
	closeAll(b, servers...)
	b.StartTimer()
}
func BenchmarkLoadThree(b *testing.B) {
	b.StopTimer()

	servers, mds := ringN(b, 3)
	client := newServer(mds)
	err := distributor.OpenReplication(client)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	size := BlockSize * 1024
	data := makeTestData(size)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		input := bytes.NewReader(data)
		volname := fmt.Sprintf("testvol-%d")

		err = block.CreateBlockVolume(client.MDS, volname, uint64(size))
		if err != nil {
			b.Fatalf("couldn't create block volume %s: %v", volname, err)
		}
		blockvol, err := block.OpenBlockVolume(client, volname)
		if err != nil {
			b.Fatalf("couldn't open block volume %s: %v", volname, err)
		}
		f, err := blockvol.OpenBlockFile()
		if err != nil {
			b.Fatalf("couldn't open blockfile %s: %v", volname, err)
		}
		copied, err := io.Copy(f, input)
		if err != nil {
			b.Fatalf("couldn't copy: %v", err)
		}
		err = f.Sync()
		if err != nil {
			b.Fatalf("couldn't sync: %v", err)
		}
		err = f.Close()
		if err != nil {
			b.Fatalf("couldn't close: %v", err)
		}

		b.Logf("copied %d bytes", copied)
	}

	b.SetBytes(int64(size))

	b.StopTimer()
	closeAll(b, servers...)
	b.StartTimer()
}
