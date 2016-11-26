package torus

import (
	"io"
	"sync"

	"golang.org/x/net/context"

	"github.com/coreos/torus/models"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	promOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "torus_server_ops_total",
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
	Blocks        BlockStore
	MDS           MetadataService
	INodes        *INodeStore
	peersMap      map[string]*models.PeerInfo
	closeChans    []chan interface{}
	Cfg           Config
	peerInfo      *models.PeerInfo
	ctx           context.Context

	lease    int64
	leaseMut sync.Mutex

	heartbeating     bool
	ReplicationOpen  bool
	timeoutCallbacks []func(string)
}

func (s *Server) createOrRenewLease(ctx context.Context) error {
	s.infoMut.Lock()
	defer s.infoMut.Unlock()
	if s.lease != 0 {
		err := s.MDS.WithContext(ctx).RenewLease(s.lease)
		if err == nil {
			return nil
		}
		clog.Errorf("Failed to renew, grant new lease for %d: %s", s.lease, err)
	}
	var err error
	s.lease, err = s.MDS.WithContext(ctx).GetLease()
	return err
}

func (s *Server) Lease() int64 {
	s.infoMut.Lock()
	defer s.infoMut.Unlock()
	return s.lease
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
	err = s.INodes.Close()
	if err != nil {
		clog.Errorf("couldn't close inodes: %s", err)
		return err
	}
	err = s.Blocks.Close()
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
	if s.ctx == nil {
		s.ctx = s.ExtendContext(context.TODO())
	}
	return s.ctx
}

func (s *Server) ExtendContext(ctx context.Context) context.Context {
	wl := context.WithValue(ctx, CtxWriteLevel, s.Cfg.WriteLevel)
	rl := context.WithValue(wl, CtxReadLevel, s.Cfg.ReadLevel)
	return rl
}

func (s *Server) GetPeerMap() map[string]*models.PeerInfo {
	s.infoMut.Lock()
	defer s.infoMut.Unlock()
	out := make(map[string]*models.PeerInfo)
	for k, v := range s.peersMap {
		out[k] = v
	}
	return out
}
