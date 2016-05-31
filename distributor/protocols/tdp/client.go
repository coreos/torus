package tdp

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/coreos/torus"
	"golang.org/x/net/context"
)

const (
	keepalive              = 2 * time.Second
	connectTimeout         = 2 * time.Second
	rebalanceClientTimeout = 5 * time.Second
	clientTimeout          = 500 * time.Millisecond
	writeClientTimeout     = 2000 * time.Millisecond
)

type request interface {
	Request() [][]byte
	GotData([]byte) (need int, res *result)
}

type result struct {
	err  error
	data [][]byte
}

type Conn struct {
	mut       sync.Mutex
	close     chan bool
	closed    bool
	err       error
	conn      net.Conn
	blockSize int
	buf       []byte
}

func Dial(addr string, timeout time.Duration, blockSize uint64) (*Conn, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	conn := &Conn{
		close:     make(chan bool),
		conn:      c,
		blockSize: int(blockSize),
		buf:       make([]byte, torus.BlockRefByteSize+1),
	}
	go conn.mainLoop()
	return conn, nil
}

func (c *Conn) mainLoop() {
	for {
		select {
		case _, ok := <-c.close:
			if !ok {
				err := c.conn.Close()
				if err != nil {
					clog.Errorf("conn closing err: %v", err)
				}
				return
			}
		case <-time.After(keepalive):
			c.mut.Lock()
			c.conn.SetDeadline(time.Now().Add(clientTimeout))
			_, err := c.conn.Write([]byte{cmdKeepAlive})
			if err != nil {
				clog.Errorf("error sending keepalive: %v", err)
				return
			}
			c.mut.Unlock()
		}
	}
}

func (c *Conn) Block(_ context.Context, ref torus.BlockRef) ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	c.conn.SetDeadline(time.Now().Add(clientTimeout))
	c.buf[0] = cmdBlock
	ref.ToBytesBuf(c.buf[1:])
	_, err := c.conn.Write(c.buf)
	if err != nil {
		fmt.Println("couldn't write")
		return nil, err
	}
	err = readConnIntoBuffer(c.conn, c.buf[:1])
	if err != nil {
		return nil, err
	}
	if c.buf[0] == respErr {
		return nil, errors.New("server error")
	}
	data := make([]byte, c.blockSize)
	err = readConnIntoBuffer(c.conn, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *Conn) PutBlock(ctx context.Context, ref torus.BlockRef, data []byte) error {
	if c.err != nil {
		return c.err
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	c.conn.SetDeadline(time.Now().Add(writeClientTimeout))
	c.buf[0] = cmdPutBlock
	ref.ToBytesBuf(c.buf[1:])
	_, err := c.conn.Write(c.buf)
	if err != nil {
		fmt.Println("couldn't write")
		return err
	}
	_, err = c.conn.Write(data)
	if err != nil {
		fmt.Println("couldn't write data")
		return err
	}
	err = readConnIntoBuffer(c.conn, c.buf[:1])
	if err != nil {
		return err
	}
	if c.buf[0] == respErr {
		return errors.New("server error")
	}
	return nil
}

func (c *Conn) RebalanceCheck(_ context.Context, refs []torus.BlockRef) ([]bool, error) {
	if c.err != nil {
		return nil, c.err
	}
	if len(refs) > 255 {
		return nil, errors.New("too many references for one request")
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	c.conn.SetDeadline(time.Now().Add(rebalanceClientTimeout))
	c.buf[0] = cmdRebalanceCheck
	c.buf[1] = byte(len(refs))
	_, err := c.conn.Write(c.buf[:2])
	if err != nil {
		fmt.Println("couldn't write")
		return nil, err
	}
	for _, ref := range refs {
		ref.ToBytesBuf(c.buf)
		_, err = c.conn.Write(c.buf[:torus.BlockRefByteSize])
		if err != nil {
			fmt.Println("couldn't write ref")
			return nil, err
		}
	}
	size := ((len(refs) - 1) / 8) + 1
	err = readConnIntoBuffer(c.conn, c.buf[:1])
	if err != nil {
		return nil, err
	}
	if c.buf[0] == respErr {
		return nil, errors.New("server error")
	}
	data := make([]byte, size)
	err = readConnIntoBuffer(c.conn, data)
	if err != nil {
		return nil, err
	}
	return bitset(data).toBool(len(refs)), nil
}

func (c *Conn) BlockSize() uint64 {
	panic("asking a connection for blocksize")
}

func (c *Conn) Err() error {
	return c.err
}

func (c *Conn) WriteBuf(_ context.Context, _ torus.BlockRef) ([]byte, error) {
	panic("wtf")
}

func (c *Conn) Close() error {
	c.mut.Lock()
	defer c.mut.Unlock()
	if !c.closed {
		close(c.close)
	}
	c.closed = true
	return nil
}
