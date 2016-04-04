package agro

import (
	"io"
	"sync"

	"golang.org/x/net/context"

	"github.com/coreos/agro/models"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	promOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_server_ops_total",
		Help: "Number of times an atomic update failed and needed to be retried",
	}, []string{"kind"})
)

func init() {
	prometheus.MustRegister(promOps)
}

const (
	CtxWriteLevel int = iota
	CtxReadLevel
)

// Server is the type representing the generic distributed block store.
type Server struct {
	mut           sync.RWMutex
	writeableLock sync.RWMutex
	infoMut       sync.Mutex
	blocks        BlockStore
	MDS           MetadataService
	inodes        *INodeStore
	peersMap      map[string]*models.PeerInfo
	closeChans    []chan interface{}
	cfg           Config
	peerInfo      *models.PeerInfo

	lease            int64
	heartbeating     bool
	replicationOpen  bool
	timeoutCallbacks []func(string)
}

func (s *Server) Close() error {
	for _, c := range s.closeChans {
		close(c)
	}
	err := s.MDS.Close()
	if err != nil {
		clog.Errorf("couldn't close mds: %s", err)
		return err
	}
	err = s.inodes.Close()
	if err != nil {
		clog.Errorf("couldn't close inodes: %s", err)
		return err
	}
	err = s.blocks.Close()
	if err != nil {
		clog.Errorf("couldn't close blocks: %s", err)
		return err
	}
	return nil
}

// Debug writes a bunch of debug output to the io.Writer.
func (s *Server) Debug(w io.Writer) error {
	if v, ok := s.MDS.(DebugMetadataService); ok {
		io.WriteString(w, "# MDS\n")
		return v.DumpMetadata(w)
	}
	return nil
}

func (s *Server) getContext() context.Context {
	return s.ExtendContext(context.TODO())
}

func (s *Server) ExtendContext(ctx context.Context) context.Context {
	wl := context.WithValue(ctx, CtxWriteLevel, s.cfg.WriteLevel)
	rl := context.WithValue(wl, CtxReadLevel, s.cfg.ReadLevel)
	return rl
}
