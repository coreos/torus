package tdp

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/torus"
	"golang.org/x/net/context"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/torus", "tdp")

var serverReadTimeout = 5 * time.Second

const (
	cmdKeepAlive byte = iota
	cmdPutBlock
	cmdBlock
	cmdRebalanceCheck
)

const (
	respOk byte = iota + 1
	respErr
)

var (
	headerOk  = []byte{respOk}
	headerErr = []byte{respErr}
)

type Server struct {
	handler   Handler
	lst       net.Listener
	blocksize uint64

	mu     sync.RWMutex // protects fields below
	closed bool
	conns  []net.Conn
}

type Handler interface {
	Block(ctx context.Context, ref torus.BlockRef) ([]byte, error)
	PutBlock(ctx context.Context, ref torus.BlockRef, data []byte) error
	RebalanceCheck(ctx context.Context, refs []torus.BlockRef) ([]bool, error)
	WriteBuf(ctx context.Context, ref torus.BlockRef) ([]byte, error)
}

var _ Handler = &Conn{}

func Serve(addr string, handler Handler, blocksize uint64) (*Server, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &Server{
		lst:       l,
		handler:   handler,
		blocksize: blocksize,
	}
	go srv.serve()
	return srv, nil
}

func (s *Server) ListenAddr() net.Addr {
	return s.lst.Addr()
}

func (s *Server) serve() {
	for {
		conn, err := s.lst.Accept()
		if err != nil {
			if !s.isClosed() {
				clog.Errorf("error listening: %v", err)
			}
			return
		}

		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()

		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	header := make([]byte, 1)
	refbuf := make([]byte, torus.BlockRefByteSize)
	null := make([]byte, s.blocksize)
	//	databuf := make([]byte, s.handler.BlockSize())
	for {
		err := readConnIntoBuffer(conn, header)
		if err != nil {
			if err == io.EOF {
				conn.Close()
				return
			}
			if !s.isClosed() {
				clog.Errorf("Error handling: %v", err)
				conn.Close()
			}
			return
		}
		switch header[0] {
		case cmdKeepAlive:
			continue
		case cmdBlock:
			err = s.handleBlock(conn, refbuf)
		case cmdPutBlock:
			err = s.handlePutBlock(conn, refbuf, null)
		case cmdRebalanceCheck:
			err := readConnIntoBuffer(conn, header)
			if err == nil {
				err = s.handleRebalanceCheck(conn, int(header[0]), refbuf)
			}
		default:
			err = errors.New("unknown message on the data port")
		}
		if err != nil {
			if !s.isClosed() {
				clog.Errorf("Error handling putblock: %v", err)
				conn.Close()
			}
			return
		}
	}
}

func readConnIntoBuffer(conn net.Conn, buf []byte) error {
	off := 0
	for off != len(buf) {
		n, err := conn.Read(buf[off:])
		if err != nil {
			return err
		}
		off = off + n
	}
	return nil
}

func (s *Server) handleBlock(conn net.Conn, refbuf []byte) error {
	err := readConnIntoBuffer(conn, refbuf)
	if err != nil {
		return err
	}
	ref := torus.BlockRefFromBytes(refbuf)
	data, err := s.handler.Block(context.TODO(), ref)
	respheader := headerOk
	if err != nil {
		respheader = headerErr
	}
	_, err = conn.Write(respheader)
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) handlePutBlock(conn net.Conn, refbuf []byte, null []byte) error {
	err := readConnIntoBuffer(conn, refbuf)
	if err != nil {
		return err
	}
	ref := torus.BlockRefFromBytes(refbuf)
	data, err := s.handler.WriteBuf(context.TODO(), ref)
	if err != nil {
		if err == torus.ErrExists {
			data = null
		} else {
			return err
		}
	}
	err = readConnIntoBuffer(conn, data)
	if err != nil {
		return err
	}
	respheader := headerOk
	if err != nil {
		respheader = headerErr
	}
	_, err = conn.Write(respheader)
	return err
}

func (s *Server) handleRebalanceCheck(conn net.Conn, len int, refbuf []byte) error {
	refs := make([]torus.BlockRef, len)
	for i := 0; i < len; i++ {
		err := readConnIntoBuffer(conn, refbuf)
		if err != nil {
			return err
		}
		refs[i] = torus.BlockRefFromBytes(refbuf)
	}
	bools, err := s.handler.RebalanceCheck(context.TODO(), refs)
	respheader := headerOk
	if err != nil {
		respheader = headerErr
	}
	_, err = conn.Write(respheader)
	if err != nil {
		return err
	}
	bs := bitsetFromBool(bools)
	_, err = conn.Write(bs)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.closed
}

func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	err := s.lst.Close()
	if err != nil {
		return err
	}

	for _, x := range s.conns {
		err := x.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
