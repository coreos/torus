// Copyright (C) 2014 Andreas Klauer <Andreas.Klauer@metamorpher.de>
// License: MIT

// Package nbd uses the Linux NBD layer to emulate a block device in user space
package nbd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"syscall"
)

const (
	// Defined in <linux/fs.h>:
	BLKROSET = 4701

	// Defined in <linux/nbd.h>:
	ioctlSetSock       = 43776
	ioctlSetBlockSize  = 43777
	ioctlDoIt          = 43779
	ioctlClearSock     = 43780
	ioctlSetSizeBlocks = 43783
	ioctlDisconnect    = 43784
	ioctlSetFlags      = 43786
)

const (
	cmdRead  = 0
	cmdWrite = 1
	cmdDisc  = 2
	cmdFlush = 3
	cmdTrim  = 4
)

const (
	flagSendFlush = (1 << 2) // can flush writeback cache
	flagSendTrim  = (1 << 5) // Send TRIM (discard)
	// flagHasFlags   = (1 << 0) // nbd-server supports flags
	// flagReadOnly   = (1 << 1) // device is read-only
	// flagSendFUA    = (1 << 3) // Send FUA (Force Unit Access)
	// flagRotational = (1 << 4) // Use elevator algorithm - rotational media
)

const (
	magicRequest = 0x25609513
	magicReply   = 0x67446698
	// Do *not* use magics: 0x12560953 0x96744668.
)

const (
	errIO = 5
)

// ioctl() helper function
func ioctl(a1, a2, a3 uintptr) (err error) {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, a1, a2, a3)
	if errno != 0 {
		err = errno
	}
	return err
}

// Device interface is a subset of os.File.
type Device interface {
	ReadAt(b []byte, off int64) (n int, err error)
	WriteAt(b []byte, off int64) (n int, err error)
	Sync() error
	Trim(off, len int64) error
}

type NBD struct {
	device    Device
	size      int64
	blocksize int64
	nbd       *os.File
	socket    int
	setsocket int
	closer    chan error
}

func Create(device Device, size int64, blocksize int64) *NBD {
	if size >= 0 {
		return &NBD{
			device:    device,
			size:      size,
			blocksize: blocksize,
			nbd:       nil,
			socket:    0,
		}
	}
	return nil
}

// return true if connected
func (nbd *NBD) IsConnected() bool {
	return nbd.nbd != nil && nbd.socket > 0
}

func (nbd *NBD) Size() int64 {
	return nbd.size
}

func (nbd *NBD) SetSize(size, blocksize int64) error {
	if err := ioctl(nbd.nbd.Fd(), ioctlSetBlockSize, uintptr(blocksize)); err != nil {
		return &os.PathError{
			Path: nbd.nbd.Name(),
			Op:   "ioctl NBD_SET_BLKSIZE",
			Err:  err,
		}
	}
	if err := ioctl(nbd.nbd.Fd(), ioctlSetSizeBlocks, uintptr(size/blocksize)); err != nil {
		return &os.PathError{
			Path: nbd.nbd.Name(),
			Op:   "ioctl NBD_SET_SIZE_BLOCKS",
			Err:  err,
		}
	}
	return nil
}

func FindDevice() (string, error) {
	// FIXME: Oh god... fixme.
	// find free nbd device
	for i := 0; ; i++ {
		dev := fmt.Sprintf("/dev/nbd%d", i)
		if _, err := os.Stat(dev); os.IsNotExist(err) {
			break // no more devices
		}
		if _, err := os.Stat(fmt.Sprintf("/sys/block/nbd%d/pid", i)); !os.IsNotExist(err) {
			continue // busy
		}
		return dev, nil
	}
	return "", errors.New("no devices available")
}

func (nbd *NBD) OpenDevice(dev string) (string, error) {
	f, err := os.Open(dev)
	if err != nil {
		return "", err
	}
	nbd.nbd = f

	// possible candidate
	ioctl(f.Fd(), BLKROSET, 0) // I'm really sorry about this
	pair, err := syscall.Socketpair(syscall.SOCK_STREAM, syscall.AF_UNIX, 0)
	if err != nil {
		return "", err
	}
	if err := ioctl(f.Fd(), ioctlSetSock, uintptr(pair[0])); err != nil {
		return "", err
	}

	nbd.setsocket = pair[0] // FIXME: We shouldn't hold on to this.
	nbd.socket = pair[1]
	return dev, nil
}

func (nbd *NBD) Serve() error {
	if err := nbd.SetSize(nbd.size, nbd.blocksize); err != nil {
		return err // already set by nbd.Size()
	}
	if err := ioctl(nbd.nbd.Fd(), ioctlSetFlags, uintptr(flagSendFlush|flagSendTrim)); err != nil {
		return &os.PathError{
			Path: nbd.nbd.Name(),
			Op:   "ioctl NBD_SET_FLAGS",
			Err:  err,
		}
	}

	c := &serverConn{
		rw: os.NewFile(uintptr(nbd.socket), "<nbd socket>"),
	}
	n := runtime.GOMAXPROCS(0)

	wg := new(sync.WaitGroup)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go c.serveLoop(nbd.device, wg)
	}
	if err := waitDisconnect(nbd.nbd); err != nil {
		return err
	}
	wg.Wait()
	return nil
}

func (nbd *NBD) Close() error {
	// FIXME: Now I understand what these did, and it terrifies me.
	// syscall.Write(nbd.setsocket, make([]byte, 28))
	// syscall.Close(nbd.setsocket)
	syscall.Close(nbd.socket)
	return nbd.nbd.Close()
}

func waitDisconnect(f *os.File) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// NBD_DO_IT does not return until disconnect
	if err := ioctl(f.Fd(), ioctlDoIt, 0); err != nil {
		return &os.PathError{
			Path: f.Name(),
			Op:   "ioctl NBD_DO_IT",
			Err:  err,
		}
	}
	if err := ioctl(f.Fd(), ioctlDisconnect, 0); err != nil {
		return &os.PathError{
			Path: f.Name(),
			Op:   "ioctl NBD_DISCONNECT",
			Err:  err,
		}
	}
	if err := ioctl(f.Fd(), ioctlClearSock, 0); err != nil {
		return &os.PathError{
			Path: f.Name(),
			Op:   "ioctl NBD_CLEAR_SOCK",
			Err:  err,
		}
	}
	return nil
}

type serverConn struct {
	mu sync.Mutex
	rw io.ReadWriteCloser
}

func (c *serverConn) serveLoop(dev Device, wg *sync.WaitGroup) error {
	if wg != nil {
		defer wg.Done()
	}

	hdr := new(reqHeader)
	buf := make([]byte, 16) // FIXME: Maybe this should be hdr[:16] instead?
	for {
		c.mu.Lock()

		if _, err := io.ReadFull(c.rw, hdr[:]); err != nil {
			// FIXME: Are there any valid short reads we need to handle?
			c.mu.Unlock()
			return err
		}

		if magic := hdr.magic(); magic != magicRequest {
			c.mu.Unlock()
			return fmt.Errorf("nbd: invalid magic: 0x%x", magic)
		}

		cmd, _ := hdr.command()
		if cmd == cmdWrite {
			buf = hdr.resize(buf)
			if _, err := io.ReadFull(c.rw, buf[16:]); err != nil {
				return err
			}
		}
		c.mu.Unlock()

		switch cmd {
		case cmdRead:
			buf = hdr.resize(buf)
			if _, err := dev.ReadAt(buf[16:], hdr.offset()); err != nil {
				hdr.putReplyHeader(buf, errIO)
			} else {
				hdr.putReplyHeader(buf, 0)
			}
		case cmdWrite:
			if _, err := dev.WriteAt(buf[16:], hdr.offset()); err != nil {
				hdr.putReplyHeader(buf, errIO)
			} else {
				hdr.putReplyHeader(buf, 0)
			}
		case cmdTrim:
			if err := dev.Trim(hdr.offset(), int64(hdr.length())); err != nil {
				log.Printf("nbd: trim error: %s", err)
			}
			fallthrough
		case cmdFlush:
			if err := dev.Sync(); err != nil {
				log.Printf("nbd: sync error: %s", err)
			}
			hdr.putReplyHeader(buf, 0)
			buf = buf[:16]
		case cmdDisc:
			// FIXME: We're actually supposed to wait for outstanding requests to finish.
			if err := dev.Sync(); err != nil {
				log.Printf("nbd: sync error: %s", err)
			}
			return c.rw.Close()
		default:
			return errors.New("nbd: invalid command")
		}

		// FIXME: Are we sure this whole write is going to be atomic?
		if _, err := c.rw.Write(buf); err != nil {
			return err
		}
	}
}

type reqHeader [28]byte

func (h *reqHeader) command() (cmd, flags uint16) {
	return binary.BigEndian.Uint16(h[6:8]), binary.BigEndian.Uint16(h[4:6])
}

func (h *reqHeader) length() uint32 {
	return binary.BigEndian.Uint32(h[24:28])
}

func (h *reqHeader) magic() uint32 {
	return binary.BigEndian.Uint32(h[0:4])
}

func (h *reqHeader) offset() int64 {
	const maxInt64 = (^uint64(0) >> 1) + 1
	if u := binary.BigEndian.Uint64(h[16:24]); u <= maxInt64 {
		return int64(u)
	}
	panic("nbd: offset overflow")
}

func (h *reqHeader) resize(buf []byte) []byte {
	if n := int(h.length()) + 16; n > cap(buf) {
		return make([]byte, n)
	} else {
		return buf[:n]
	}
}

func (h *reqHeader) putReplyHeader(dst []byte, err uint32) {
	binary.BigEndian.PutUint32(dst[0:4], magicReply)
	binary.BigEndian.PutUint32(dst[4:8], err)
	copy(dst[8:16], h[8:16])
}
