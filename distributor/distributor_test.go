package distributor

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/metadata/temp"

	_ "github.com/coreos/agro/storage"
)

func newServer(md *temp.Server) *agro.Server {
	cfg := agro.Config{
		StorageSize: 100 * 1024 * 1024,
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
		addr := fmt.Sprintf("http://127.0.0.1:%d", 40000+i)
		uri, err := url.Parse(addr)
		if err != nil {
			t.Fatal(err)
		}
		err = ListenReplication(srv, uri)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, srv)
	}
	// Heartbeat
	time.Sleep(10 * time.Millisecond)
	return out, s
}

func closeAll(t *testing.T, c ...*agro.Server) {
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
}

func testOpenClose(t *testing.T) {
	srvs, md := createThree(t)
	p, err := srvs[0].MDS.GetPeers()
	if err != nil {
		t.Fatal(err)
	}
	if len(p) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(p))
	}
	closeAll(t, srvs...)
	md.Close()
}
