package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/metadata/temp"
	"github.com/barakmich/agro/models"
)

func newServer(md *temp.Server) *server {
	cfg := agro.Config{}
	mds := temp.NewClient(cfg, md)
	inodes, _ := agro.CreateINodeStore("temp", cfg)
	gmd, _ := mds.GlobalMetadata()
	cold, _ := agro.CreateBlockStore("temp", cfg, gmd)
	return &server{
		blocks:   cold,
		mds:      mds,
		inodes:   inodes,
		peersMap: make(map[string]*models.PeerInfo),
	}
}

func createThree(t *testing.T) ([]*server, *temp.Server) {
	var out []*server
	s := temp.NewServer()
	for i := 0; i < 3; i++ {
		srv := newServer(s)
		srv.ListenReplication(fmt.Sprintf("127.0.0.1:%d", 40000+i))
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

func TestOpenClose(t *testing.T) {
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
