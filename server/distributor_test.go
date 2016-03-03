package server

import (
	"crypto/rand"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/metadata/temp"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"
)

func newServer(md *temp.Server) *server {
	cfg := agro.Config{}
	mds := temp.NewClient(cfg, md)
	gmd, _ := mds.GlobalMetadata()
	blocks, _ := agro.CreateBlockStore("temp", "current", cfg, gmd)
	return &server{
		blocks:         blocks,
		mds:            mds,
		inodes:         NewINodeStore(blocks),
		peersMap:       make(map[string]*models.PeerInfo),
		openINodeRefs:  make(map[string]map[agro.INodeID]int),
		peerInfo:       &models.PeerInfo{UUID: mds.UUID()},
		openFileChains: make(map[uint64]openFileCount),
	}
}

func createThree(t *testing.T) ([]*server, *temp.Server) {
	var out []*server
	s := temp.NewServer()
	for i := 0; i < 3; i++ {
		srv := newServer(s)
		err := srv.ListenReplication(fmt.Sprintf("127.0.0.1:%d", 40000+i))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, srv)
	}
	// Heartbeat
	time.Sleep(10 * time.Millisecond)
	return out, s
}

func closeAll(t *testing.T, c ...*server) {
	for _, x := range c {
		err := x.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestWithThree(t *testing.T) {
	t.Log("OpenClose")
	testOpenClose(t)
	time.Sleep(100 * time.Millisecond)
	t.Log("Write")
	testThreeWrite(t)
}

func testOpenClose(t *testing.T) {
	srvs, md := createThree(t)
	p, err := srvs[0].mds.GetPeers()
	if err != nil {
		t.Fatal(err)
	}
	if len(p) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(p))
	}
	closeAll(t, srvs...)
	md.Close()
}

func testThreeWrite(t *testing.T) {
	srvs, md := createThree(t)
	p, _ := srvs[0].mds.GetPeers()
	defer md.Close()
	defer closeAll(t, srvs...)
	r, err := ring.CreateRing(&models.Ring{
		Type:    uint32(ring.Mod),
		Peers:   p,
		Version: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	md.SetRing(r)
	for {
		ok := true
		for i := 0; i < 3; i++ {
			d := srvs[i].blocks.(*distributor)
			if d.ring.Version() != 2 {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	srvs[1].mds.CreateVolume("vol1")
	err = srvs[1].Mkdir(agro.Path{
		Volume: "vol1",
		Path:   "/foo/",
	}, &models.Metadata{})
	if err != nil {
		t.Errorf("couldn't mkdir: %s", err)
		return
	}
	testPath := agro.Path{
		Volume: "vol1",
		Path:   "/foo/bar",
	}
	f, err := srvs[2].Create(testPath)
	if err != nil {
		t.Errorf("couldn't open: %s", err)
		return
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
	f2, err := srvs[0].Open(testPath)
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
