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
	inodes, _ := agro.CreateINodeStore("temp", cfg)
	gmd, _ := mds.GlobalMetadata()
	cold, _ := agro.CreateBlockStore("temp", cfg, gmd)
	return &server{
		blocks:        cold,
		mds:           mds,
		inodes:        inodes,
		peersMap:      make(map[string]*models.PeerInfo),
		openINodeRefs: make(map[string]map[agro.INodeID]int),
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
	var uuids []string
	defer md.Close()
	defer closeAll(t, srvs...)
	for _, x := range p {
		uuids = append(uuids, x.UUID)
	}
	md.SetRing(&models.Ring{
		Type:    uint32(ring.Mod),
		UUIDs:   uuids,
		Version: 2,
	})
	for {
		ok := true
		for i := 0; i < 3; i++ {
			d := srvs[i].inodes.(*distributor)
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
	testPath := agro.Path{
		Volume: "vol1",
		Path:   "/foo",
	}

	f, err := srvs[2].Create(testPath, models.Metadata{})
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
