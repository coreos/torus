package nbd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/coreos/pkg/capnslog"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "nbd")

const (
	nbdMagic       uint64 = 0x4e42444d41474943
	nbdOpts        uint64 = 0x49484156454F5054
	nbdRequestSize        = (4 + 4 + 8 + 8 + 4)

	nbdFlagFixedNewStyle = 0x1

	// options
	nbdOptExportName = 0x1
	nbdOptAbort      = 0x2
	nbdOptList       = 0x3

	ioKeepAlive = 10 * time.Second
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
		c.SetKeepAlivePeriod(ioKeepAlive)

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

	return nil
}

func (s *NBDServer) Close() error {
	return s.l.Close()
}

type option struct {
	opt  uint32
	data []byte
}

type NBDConn struct {
	c      *net.TCPConn
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
	var dev Device
	var handshakeFlags uint16
	var clientFlags uint32

	c.tracef("handshaking")

	// handshake
	handshakeFlags = nbdFlagFixedNewStyle

	if err := binary.Write(c.c, binary.BigEndian, nbdMagic); err != nil {
		return err
	}

	if err := binary.Write(c.c, binary.BigEndian, nbdOpts); err != nil {
		return err
	}

	if err := binary.Write(c.c, binary.BigEndian, handshakeFlags); err != nil {
		return err
	}

	if err := binary.Read(c.c, binary.BigEndian, &clientFlags); err != nil {
		return err
	}

	c.tracef("reading options")

	// read options
optLoop:
	for {
		opt, err := c.getOpt()
		if err != nil {
			return err
		}

		switch opt.opt {
		case nbdOptExportName:
			dev, err = c.finder.FindDevice(string(opt.data))
			if err != nil {
				// terminate the connection on failure
				return err
			}

			c.device = dev
			c.export = string(opt.data)

			// got dev, run connection.
			break optLoop
		case nbdOptAbort:
			if err := c.optReply(nbdOptList, NBD_REP_ACK, nil); err != nil {
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
				if err := c.optReply(nbdOptList, NBD_REP_SERVER, buf); err != nil {
					return err
				}
			}

			if err := c.optReply(nbdOptList, NBD_REP_ACK, nil); err != nil {
				return err
			}
		default:
			if err := c.optReply(opt.opt, NBD_REP_ERR_UNSUP, nil); err != nil {
				return err
			}
		}
	}

	// send transmission flags
	if err := binary.Write(c.c, binary.BigEndian, c.device.Size()); err != nil {
		return err
	}

	if err := binary.Write(c.c, binary.BigEndian, uint16(NBD_FLAG_HAS_FLAGS|NBD_FLAG_SEND_FLUSH|NBD_FLAG_SEND_TRIM)); err != nil {
		return err
	}

	// reserved zero pad
	zpad := make([]byte, 124)
	if err := writeFull(c.c, zpad); err != nil {
		return err
	}

	c.tracef("serving")

	return c.mainloop()
}

func writeFull(w io.Writer, buf []byte) error {
	n, err := w.Write(buf)
	if err != nil {
		return err
	}

	if n != len(buf) {
		return errors.New("short write")
	}

	return nil
}

func (c *NBDConn) mainloop() error {
	buf := make([]byte, 2<<19)
	var x request

	defer func() {
		// We're done
		err := c.device.Sync()
		if err != nil {
			c.errorf("sync error: %v", err)
		}
	}()

	for {
		if _, err := io.ReadFull(c.c, buf[0:28]); err != nil {
			return err
		}

		x.magic = binary.BigEndian.Uint32(buf)
		x.typus = binary.BigEndian.Uint32(buf[4:8])
		x.handle = binary.BigEndian.Uint64(buf[8:16])
		x.from = binary.BigEndian.Uint64(buf[16:24])
		x.len = binary.BigEndian.Uint32(buf[24:28])

		switch x.magic {
		case NBD_REQUEST_MAGIC:
			if clog.LevelAt(capnslog.TRACE) {
				c.tracef("command: %v", cmdString(x.typus))
			}

			switch x.typus {
			case NBD_CMD_READ:
				binary.BigEndian.PutUint32(buf[0:4], NBD_REPLY_MAGIC)
				binary.BigEndian.PutUint32(buf[4:8], 0)

				n, err := c.device.ReadAt(buf[16:16+x.len], int64(x.from))
				if err != nil {
					c.errorf("read failure: %v", err)
					binary.BigEndian.PutUint32(buf[4:8], NBD_EIO)
				} else if n != int(x.len) {
					binary.BigEndian.PutUint32(buf[4:8], NBD_EIO)
				}

				if err := writeFull(c.c, buf[0:16+x.len]); err != nil {
					return err
				}
			case NBD_CMD_WRITE:
				binary.BigEndian.PutUint32(buf[0:4], NBD_REPLY_MAGIC)
				binary.BigEndian.PutUint32(buf[4:8], 0)

				n, err := c.c.Read(buf[28 : 28+x.len])
				if err != nil {
					return err
				}

				for uint32(n) < x.len {
					m, err := c.c.Read(buf[28+n : 28+x.len])
					if err != nil {
						return err
					}
					n += m
				}

				n, err = c.device.WriteAt(buf[28:28+x.len], int64(x.from))
				if err != nil {
					c.errorf("write failure: %v", err)
					binary.BigEndian.PutUint32(buf[4:8], NBD_EIO)
				} else if n != int(x.len) {
					binary.BigEndian.PutUint32(buf[4:8], NBD_EIO)
				}

				if err := writeFull(c.c, buf[0:16]); err != nil {
					return err
				}
			case NBD_CMD_DISC:
				if err := c.device.Sync(); err != nil {
					c.errorf("sync error: %v", err)
				}

				// we're done here
				return nil
			case NBD_CMD_TRIM:
				binary.BigEndian.PutUint32(buf[0:4], NBD_REPLY_MAGIC)
				binary.BigEndian.PutUint32(buf[4:8], 0)

				if err := c.device.Trim(int64(x.from), int64(x.len)); err != nil {
					c.errorf("trim error: %v", err)
					binary.BigEndian.PutUint32(buf[4:8], NBD_EIO)
				}

				if err := c.device.Sync(); err != nil {
					c.errorf("sync error: %v", err)
					binary.BigEndian.PutUint32(buf[4:8], NBD_EIO)
				}

				if err := writeFull(c.c, buf[0:16]); err != nil {
					return err
				}
			case NBD_CMD_FLUSH:
				binary.BigEndian.PutUint32(buf[0:4], NBD_REPLY_MAGIC)
				binary.BigEndian.PutUint32(buf[4:8], 0)

				if err := c.device.Sync(); err != nil {
					c.errorf("sync error: %v", err)
					binary.BigEndian.PutUint32(buf[4:8], NBD_EIO)
				}

				if err := writeFull(c.c, buf[0:16]); err != nil {
					return err
				}
			default:
				// try to sanely handle unrecognized commands
				binary.BigEndian.PutUint32(buf[0:4], NBD_REPLY_MAGIC)
				binary.BigEndian.PutUint32(buf[4:8], NBD_EINVAL)

				if err := writeFull(c.c, buf[0:16]); err != nil {
					return err
				}
			}
		default:
			// unknown magic, terminate.
			return fmt.Errorf("bad command magic: %x", x.magic)
		}
	}
}

func (c *NBDConn) optReply(opt uint32, reptype uint32, data []byte) error {
	rep := make([]byte, 8+4+4+4)

	// option reply magic
	binary.BigEndian.PutUint64(rep[0:8], uint64(0x3e889045565a9))
	binary.BigEndian.PutUint32(rep[8:12], opt)
	binary.BigEndian.PutUint32(rep[12:16], reptype)
	binary.BigEndian.PutUint32(rep[16:20], uint32(len(data)))

	if len(data) > 0 {
		rep = append(rep, data...)
	}

	return writeFull(c.c, rep)
}

func (c *NBDConn) getOpt() (*option, error) {
	var optMagic uint64
	var opt uint32
	var optLen uint32

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
