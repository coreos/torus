package nbd

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/coreos/pkg/capnslog"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/torus", "nbd")

const (
	nbdMagic uint64 = 0x4e42444d41474943
	nbdOpts  uint64 = 0x49484156454F5054

	nbdFlagFixedNewStyle = 0x1

	// options
	nbdOptExportName = 0x1
	nbdOptAbort      = 0x2
	nbdOptList       = 0x3

	// option replies
	nbdRepAck      = 0x1
	nbdRepServer   = 0x2
	nbdRepErrUnsup = 0x80000001

	tcpKeepAlive = 10 * time.Second
)

type DeviceFinder interface {
	FindDevice(name string) (Device, error)
	ListDevices() ([]string, error)
}

type NBDServer struct {
	l      *net.TCPListener
	finder DeviceFinder
}

func NewNBDServer(addr string, finder DeviceFinder) (*NBDServer, error) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	ln, err := net.ListenTCP("tcp", tcpaddr)
	if err != nil {
		return nil, err
	}

	ns := &NBDServer{
		l:      ln,
		finder: finder,
	}

	return ns, nil
}

func (s *NBDServer) Serve() error {
	for {
		c, err := s.l.AcceptTCP()
		if err != nil {
			return err
		}

		c.SetKeepAlive(true)
		c.SetKeepAlivePeriod(tcpKeepAlive)

		conn := &NBDConn{
			c:      c,
			finder: s.finder,
			export: "<none>",
		}

		go func() {
			if err := conn.handler(); err != nil {
				conn.errorf("handler error: %v", err)
			}
			conn.Close()
		}()
	}
}

func (s *NBDServer) Close() error {
	return s.l.Close()
}

type option struct {
	opt  uint32
	data []byte
}

type NBDConn struct {
	c      net.Conn
	finder DeviceFinder
	device Device
	export string
}

func (c *NBDConn) errorf(format string, stuff ...interface{}) {
	s := fmt.Sprintf(format, stuff...)
	clog.Errorf("%s: %s: %s", c.c.RemoteAddr(), c.export, s)
}

func (c *NBDConn) tracef(format string, stuff ...interface{}) {
	s := fmt.Sprintf(format, stuff...)
	clog.Tracef("%s: %s: %s", c.c.RemoteAddr(), c.export, s)
}

func (c *NBDConn) handler() error {
	var clientFlags uint32

	c.tracef("handshaking")

	// initial handshake
	if err := binary.Write(c.c, binary.BigEndian, nbdMagic); err != nil {
		return err
	}

	if err := binary.Write(c.c, binary.BigEndian, nbdOpts); err != nil {
		return err
	}

	if err := binary.Write(c.c, binary.BigEndian, uint16(nbdFlagFixedNewStyle)); err != nil {
		return err
	}

	// these are read but ignore since none are used.
	if err := binary.Read(c.c, binary.BigEndian, &clientFlags); err != nil {
		return err
	}

	c.tracef("reading options")

	// get options
	if err := c.options(); err != nil {
		return fmt.Errorf("handshake failure: %v", err)
	}

	// send transmission flags
	if err := binary.Write(c.c, binary.BigEndian, c.device.Size()); err != nil {
		return err
	}

	if err := binary.Write(c.c, binary.BigEndian, uint16(flagHasFlags|flagSendFlush|flagSendTrim)); err != nil {
		return err
	}

	// reserved zero pad
	zpad := make([]byte, 124)
	if err := writeFull(c.c, zpad); err != nil {
		return err
	}

	c.tracef("serving")

	srv := &serverConn{
		rw: c.c,
	}

	n := 1

	wg := new(sync.WaitGroup)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			if err := srv.serveLoop(c.device, wg); err != nil {
				clog.Errorf("server returned: %s", err)
			}
		}()
	}

	wg.Wait()
	return nil
}

// do nbd option exchange and return once export name is given.
func (c *NBDConn) options() error {
	for {
		opt, err := c.getOpt()
		if err != nil {
			return err
		}

		switch opt.opt {
		case nbdOptExportName:
			if len(opt.data) == 0 {
				return fmt.Errorf("nbdserve doesn't support empty volume name. client needs to specify it")
			}
			dev, err := c.finder.FindDevice(string(opt.data))
			if err != nil {
				// terminate the connection on failure
				return err
			}

			c.device = dev
			c.export = string(opt.data)

			// got dev, done with options.
			return nil
		case nbdOptAbort:
			if err := c.optReply(nbdOptList, nbdRepAck, nil); err != nil {
				return err
			}

			return nil
		case nbdOptList:
			devs, err := c.finder.ListDevices()
			if err != nil {
				return err
			}

			for _, d := range devs {
				// a bit silly. prefix string with length..
				buf := make([]byte, 4+len(d))
				binary.BigEndian.PutUint32(buf[0:4], uint32(len(d)))
				copy(buf[4:], []byte(d))
				if err := c.optReply(nbdOptList, nbdRepServer, buf); err != nil {
					return err
				}
			}

			if err := c.optReply(nbdOptList, nbdRepAck, nil); err != nil {
				return err
			}
		default:
			if err := c.optReply(opt.opt, nbdRepErrUnsup, nil); err != nil {
				return err
			}
		}
	}
}

func writeFull(w io.Writer, buf []byte) error {
	n, err := w.Write(buf)
	if err != nil {
		return err
	}

	if n != len(buf) {
		return io.ErrShortWrite
	}

	return nil
}

func (c *NBDConn) optReply(opt uint32, reptype uint32, data []byte) error {
	rep := make([]byte, 8+4+4+4)

	// option reply magic
	binary.BigEndian.PutUint64(rep[0:8], uint64(0x3e889045565a9))
	binary.BigEndian.PutUint32(rep[8:12], opt)
	binary.BigEndian.PutUint32(rep[12:16], reptype)
	binary.BigEndian.PutUint32(rep[16:20], uint32(len(data)))

	rep = append(rep, data...)

	return writeFull(c.c, rep)
}

func (c *NBDConn) getOpt() (*option, error) {
	var (
		optMagic uint64
		opt      uint32
		optLen   uint32
	)

	if err := binary.Read(c.c, binary.BigEndian, &optMagic); err != nil {
		return nil, err
	}

	if optMagic != nbdOpts {
		return nil, fmt.Errorf("bad option magic %x", optMagic)
	}

	if err := binary.Read(c.c, binary.BigEndian, &opt); err != nil {
		return nil, err
	}

	if err := binary.Read(c.c, binary.BigEndian, &optLen); err != nil {
		return nil, err
	}

	if optLen > 4096 {
		return nil, fmt.Errorf("strange option length: %d", optLen)
	}

	switch opt {
	case nbdOptExportName:
		o := &option{
			opt:  opt,
			data: make([]byte, optLen),
		}

		if optLen > 0 {
			if _, err := io.ReadFull(c.c, o.data); err != nil {
				return nil, err
			}
		}

		return o, nil
	case nbdOptAbort, nbdOptList:
		return &option{opt: opt}, nil
	default:
		return nil, fmt.Errorf("unknown option %032b", opt)
	}
}

func (c *NBDConn) Close() error {
	if c.device != nil {
		c.device.Close()
	}
	return c.c.Close()
}
