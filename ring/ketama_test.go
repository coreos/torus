package ring

import (
	"testing"

	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
	"github.com/serialx/hashring"
)

func TestTinyPeer(t *testing.T) {
	pi := torus.PeerInfoList{
		&models.PeerInfo{
			UUID:        "a",
			TotalBlocks: 20 * 1024 * 1024 * 2,
		},
		&models.PeerInfo{
			UUID:        "b",
			TotalBlocks: 20 * 1024 * 1024 * 2,
		},
		&models.PeerInfo{
			UUID:        "c",
			TotalBlocks: 100 * 1024 * 2,
		},
	}
	k := &ketama{
		version: 1,
		peers:   pi,
		rep:     2,
		ring:    hashring.NewWithWeights(pi.GetWeights()),
	}
	l, err := k.GetPeers(torus.BlockRef{
		INodeRef: torus.NewINodeRef(3, 4),
		Index:    5,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(l.Peers)
}
