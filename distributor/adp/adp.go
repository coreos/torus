package adp

import (
	"io"
	"net"
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/pkg/capnslog"
	"golang.org/x/net/context"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "adp")

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
	handler Handler
	lst     net.Listener
	conns   []net.Conn
	closed  bool
}

type Handler interface {
	Block(ctx context.Context, ref agro.BlockRef) ([]byte, error)
	PutBlock(ctx context.Context, ref agro.BlockRef, data []byte) error
	RebalanceCheck(ctx context.Context, refs []agro.BlockRef) ([]bool, error)
	BlockSize() uint64
}

var _ Handler = &Conn{}

func Serve(addr string, handler Handler) (*Server, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &Server{
		lst:     l,
		handler: handler,
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
			if !s.closed {
				clog.Errorf("error listening: %v", err)
			}
			return
		}
		s.conns = append(s.conns, conn)
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	header := make([]byte, 1)
	refbuf := make([]byte, agro.BlockRefByteSize)
	for {
		err := readConnIntoBuffer(conn, header)
		if err != nil {
			if !s.closed {
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
			err = s.handlePutBlock(conn, refbuf)
		case cmdRebalanceCheck:
			conn.SetReadDeadline(time.Now().Add(serverReadTimeout))
			_, err = conn.Read(header)
			if err == nil {
				err = s.handleRebalanceCheck(conn, int(header[0]), refbuf)
			}
		default:
			panic("unhandled")
		}
		if err != nil {
			if !s.closed {
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
		conn.SetReadDeadline(time.Now().Add(serverReadTimeout))
		n, err := conn.Read(buf[off:])
		if err != nil && err != io.EOF {
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
	ref := agro.BlockRefFromBytes(refbuf)
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

func (s *Server) handlePutBlock(conn net.Conn, refbuf []byte) error {
	err := readConnIntoBuffer(conn, refbuf)
	if err != nil {
		return err
	}
	data := make([]byte, int(s.handler.BlockSize()))
	err = readConnIntoBuffer(conn, data)
	if err != nil {
		return err
	}
	ref := agro.BlockRefFromBytes(refbuf)
	err = s.handler.PutBlock(context.TODO(), ref, data)
	respheader := headerOk
	if err != nil {
		respheader = headerErr
	}
	_, err = conn.Write(respheader)
	return err
}

func (s *Server) handleRebalanceCheck(conn net.Conn, len int, refbuf []byte) error {
	refs := make([]agro.BlockRef, len)
	for i := 0; i < len; i++ {
		err := readConnIntoBuffer(conn, refbuf)
		if err != nil {
			return err
		}
		refs[i] = agro.BlockRefFromBytes(refbuf)
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

func (s *Server) Close() error {
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
