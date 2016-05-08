package adp

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/coreos/agro"
	"golang.org/x/net/context"
)

var keepalive = 2 * time.Second

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
	err       error
	conn      net.Conn
	blockSize int
	singleBuf []byte
	refBuf    []byte
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
		singleBuf: make([]byte, 1),
		refBuf:    make([]byte, agro.BlockRefByteSize),
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
			_, err := c.conn.Write([]byte{cmdKeepAlive})
			if err != nil {
				clog.Errorf("error sending keepalive: %v", err)
				return
			}
			c.mut.Unlock()
		}
	}
}

func (c *Conn) Block(_ context.Context, ref agro.BlockRef) ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	c.singleBuf[0] = cmdBlock
	_, err := c.conn.Write(c.singleBuf)
	if err != nil {
		fmt.Println("couldn't write")
		return nil, err
	}
	ref.ToBytesBuf(c.refBuf)
	_, err = c.conn.Write(c.refBuf)
	if err != nil {
		fmt.Println("couldn't write")
		return nil, err
	}
	err = readConnIntoBuffer(c.conn, c.singleBuf)
	if err != nil {
		return nil, err
	}
	if c.singleBuf[0] == respErr {
		return nil, errors.New("server error")
	}
	data := make([]byte, c.blockSize)
	err = readConnIntoBuffer(c.conn, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *Conn) PutBlock(ctx context.Context, ref agro.BlockRef, data []byte) error {
	if c.err != nil {
		return c.err
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	c.singleBuf[0] = cmdPutBlock
	_, err := c.conn.Write(c.singleBuf)
	if err != nil {
		fmt.Println("couldn't write")
		return err
	}
	ref.ToBytesBuf(c.refBuf)
	_, err = c.conn.Write(c.refBuf)
	if err != nil {
		fmt.Println("couldn't write")
		return err
	}
	_, err = c.conn.Write(data)
	if err != nil {
		fmt.Println("couldn't write data")
		return err
	}
	err = readConnIntoBuffer(c.conn, c.singleBuf)
	if err != nil {
		return err
	}
	if c.singleBuf[0] == respErr {
		return errors.New("server error")
	}
	return nil
}

func (c *Conn) RebalanceCheck(_ context.Context, refs []agro.BlockRef) ([]bool, error) {
	if c.err != nil {
		return nil, c.err
	}
	if len(refs) > 255 {
		return nil, errors.New("too many references for one request")
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	c.singleBuf[0] = cmdRebalanceCheck
	_, err := c.conn.Write(c.singleBuf)
	if err != nil {
		fmt.Println("couldn't write")
		return nil, err
	}
	c.singleBuf[0] = byte(len(refs))
	_, err = c.conn.Write(c.singleBuf)
	if err != nil {
		fmt.Println("couldn't write")
		return nil, err
	}
	for _, ref := range refs {
		ref.ToBytesBuf(c.refBuf)
		_, err = c.conn.Write(c.refBuf)
		if err != nil {
			fmt.Println("couldn't write ref")
			return nil, err
		}
	}
	size := ((len(refs) - 1) / 8) + 1
	err = readConnIntoBuffer(c.conn, c.singleBuf)
	if err != nil {
		return nil, err
	}
	if c.singleBuf[0] == respErr {
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

func (c *Conn) Close() error {
	close(c.close)
	return nil
}
