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

func createThree(t *testing.T) ([]*agro.Server, *temp.Server) {
	var out []*agro.Server
	s := temp.NewServer()
	for i := 0; i < 3; i++ {
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

func ringThree(t *testing.T) ([]*agro.Server, *temp.Server) {
	servers, mds := createThree(t)
	var peers agro.PeerInfoList
	for _, s := range servers {
		peers = append(peers, &models.PeerInfo{
			UUID:        s.MDS.UUID(),
			TotalBlocks: StorageSize / BlockSize,
		})
	}
	newRing, err := ring.CreateRing(&models.Ring{
		Type:              uint32(ring.Ketama),
		Peers:             peers,
		ReplicationFactor: 2,
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

func closeAll(t *testing.T, c ...*agro.Server) {
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
	servers, mds := ringThree(t)
	client := newServer(mds)
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
	reader := newServer(mds)
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
	servers, mds := ringThree(t)
	client := newServer(mds)
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
