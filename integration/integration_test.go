package agro

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
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
	StorageSize = 512 * 1024 * 1024
	BlockSize   = 256 * 1024
)

func newServer(t testing.TB, md *temp.Server) *agro.Server {
	dir, _ := ioutil.TempDir("", "agro-integration")
	agro.MkdirsFor(dir)
	cfg := agro.Config{
		StorageSize: StorageSize,
		DataDir:     dir,
	}
	mds := temp.NewClient(cfg, md)
	gmd, _ := mds.GlobalMetadata()
	blocks, err := agro.CreateBlockStore("mfile", "current", cfg, gmd)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := agro.NewServerByImpl(cfg, mds, blocks)
	return s
}

func createN(t testing.TB, n int) ([]*agro.Server, *temp.Server) {
	var out []*agro.Server
	s := temp.NewServer()
	for i := 0; i < n; i++ {
		srv := newServer(t, s)
		addr := fmt.Sprintf("adp://127.0.0.1:%d", 40000+i)
		uri, err := url.Parse(addr)
		if err != nil {
			t.Fatal(err)
		}
		err = distributor.ListenReplication(srv, uri)
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
		err = os.RemoveAll(x.Cfg.DataDir)
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

func createVol(t *testing.T, server *agro.Server, volname string, size uint64) *block.BlockFile {
	err := block.CreateBlockVolume(server.MDS, volname, size)
	if err != nil {
		t.Fatalf("couldn't create block volume %s: %v", volname, err)
	}
	return openVol(t, server, volname)
}

func openVol(t *testing.T, server *agro.Server, volname string) *block.BlockFile {
	blockvol, err := block.OpenBlockVolume(server, volname)
	if err != nil {
		t.Fatalf("couldn't open block volume %s: %v", volname, err)
	}
	f, err := blockvol.OpenBlockFile()
	if err != nil {
		t.Fatalf("couldn't open blockfile %s: %v", volname, err)
	}
	return f
}

func TestLoadAndDump(t *testing.T) {
	servers, mds := ringN(t, 3)
	client := newServer(t, mds)
	err := distributor.OpenReplication(client)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	size := BlockSize * 100
	data := makeTestData(size)
	input := bytes.NewReader(data)
	f := createVol(t, client, "testvol", uint64(size))
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

	compareBytes(t, mds, data, "testvol")
	closeAll(t, servers...)
}

func compareBytes(t *testing.T, mds *temp.Server, data []byte, volume string) {
	reader := newServer(t, mds)
	err := distributor.OpenReplication(reader)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	f := openVol(t, reader, "testvol")
	output := &bytes.Buffer{}
	_, err = io.Copy(output, f)
	if err != nil {
		t.Fatalf("couldn't copy: %v", err)
	}
	if !bytes.Equal(output.Bytes(), data) {
		t.Error("bytes not equal")
	}
	f.Close()
}

func TestRewrite(t *testing.T) {
	servers, mds := ringN(t, 3)
	client := newServer(t, mds)
	err := distributor.OpenReplication(client)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	size := BlockSize * 100
	data := makeTestData(size)
	input := bytes.NewReader(data)
	f := createVol(t, client, "testvol", uint64(size))

	for i := 0; i <= 100; i++ {
		input.Seek(int64(i%100), 0)
		f.Seek(int64(i%100), 0)
		_, err := io.Copy(f, input)
		if err != nil {
			t.Fatalf("couldn't copy: %v", err)
		}
		err = f.Sync()
		if err != nil {
			t.Fatalf("couldn't sync: %v", err)
		}
	}

	err = f.Close()
	if err != nil {
		t.Fatalf("couldn't close: %v", err)
	}
	compareBytes(t, mds, data, "testvol")
	closeAll(t, servers...)
}

func BenchmarkLoadOne(b *testing.B) {
	b.StopTimer()

	servers, mds := ringN(b, 1)
	client := newServer(b, mds)
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
	client := newServer(b, mds)
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
