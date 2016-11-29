// Copyright (C) 2014 Andreas Klauer <Andreas.Klauer@metamorpher.de>
// License: MIT

// Package nbd uses the Linux NBD layer to emulate a block device in user space
package nbd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"
)

const (
	// Defined in <linux/fs.h>:
	BLKROSET = 4701

	// Defined in <linux/nbd.h>:
	ioctlSetSock       = 43776
	ioctlSetBlockSize  = 43777
	ioctlSetSize       = 43778
	ioctlDoIt          = 43779
	ioctlClearSock     = 43780
	ioctlClearQueue    = 43781
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
	flagHasFlags  = (1 << 0) // nbd-server supports flags
	flagSendFlush = (1 << 2) // can flush writeback cache
	flagSendTrim  = (1 << 5) // Send TRIM (discard)
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
	Size() uint64
	Close() error
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

func (nbd *NBD) SetSize(size int64) error {
	if err := ioctl(nbd.nbd.Fd(), ioctlSetSize, uintptr(size)); err != nil {
		return &os.PathError{
			Path: nbd.nbd.Name(),
			Op:   "ioctl NBD_SET_SIZE",
			Err:  err,
		}
	}
	return nil
}

func (nbd *NBD) SetBlockSize(blocksize int64) error {
	if err := ioctl(nbd.nbd.Fd(), ioctlSetBlockSize, uintptr(blocksize)); err != nil {
		return &os.PathError{
			Path: nbd.nbd.Name(),
			Op:   "ioctl NBD_SET_BLKSIZE",
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
	pair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
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
	blksized := true
	if err := nbd.SetSize(nbd.size); err != nil {
		return err // already set by nbd.Size()
	}
	if err := nbd.SetBlockSize(nbd.blocksize); err != nil {
		// This is a hack around the changes made to the kernel in 4.6
		// (particularly commit 37091fdd831f28a6509008542174ed324dd645bc)
		// -- because the size of the device is cached at 0, the blocksize can't change
		// until connected. So we'll do a workaround on newer kernels, but man, it'd be
		// nice to fix this. There needs to be a little better logic kernel-side around changing size
		// even when disconnected. Changing it only when connected is fine -- but keep my intent.
		blksized = false
	}
	if err := ioctl(nbd.nbd.Fd(), ioctlSetFlags, uintptr(flagSendFlush|flagSendTrim)); err != nil {
		switch err {
		case syscall.ENOTTY:
			clog.Error(fmt.Sprintf("ioctl returned: %v. kernel version may be old. flush thread will run every 30sec", err))
			go func() {
				for {
					time.Sleep(30 * time.Second)
					if err := nbd.device.Sync(); err != nil {
						clog.Printf("sync error: %s", err)
					}
				}
			}()
		default:
			return &os.PathError{
				Path: nbd.nbd.Name(),
				Op:   "ioctl NBD_SET_FLAGS",
				Err:  err,
			}
		}
	}

	fmt.Printf("Attached to %s. Server loop begins ... \n", nbd.nbd.Name())
	c := &serverConn{
		rw: os.NewFile(uintptr(nbd.socket), "<nbd socket>"),
	}
	// TODO(barakmich): Scale up NBD by handling multiple requests.
	// Requires thread-safety across the block.BlockFile/torus.File
	//n := runtime.GOMAXPROCS(0) - 1
	n := 1

	wg := new(sync.WaitGroup)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			if err := c.serveLoop(nbd.device, wg); err != nil {
				clog.Errorf("server returned: %s", err)
			}
		}()
	}
	if !blksized {
		// Back to the hack.
		go func(nbd *NBD) {
			// Hopefully we'll be connected in 500 millis.
			// If not, we'll proceed with the standard blocksize of 1K.
			time.Sleep(time.Microsecond * 500)
			err := nbd.SetBlockSize(nbd.blocksize)
			if err != nil {
				clog.Printf("Couldn't upgrade blocksize: %s", err)
			}
		}(nbd)
	}

	// NBD_DO_IT does not return until disconnect
	if err := ioctl(nbd.nbd.Fd(), ioctlDoIt, 0); err != nil {
		clog.Errorf("error %s: ioctl returned %v", nbd.nbd.Name(), err)
	}

	wg.Wait()
	return nil
}

func Detach(dev string) error {
	f, err := os.Open(dev)
	if err != nil {
		return err
	}
	n := &NBD{nbd: f}
	n.Disconnect()
	return nil
}

func (nbd *NBD) Disconnect() {
	var err error
	if nbd.nbd == nil {
		return
	}

	clog.Infof("Running disconnection to %s", nbd.nbd.Name())

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	err = ioctl(nbd.nbd.Fd(), ioctlDisconnect, 0)
	if err != nil {
		clog.Errorf("error disconnecting %s. ioctl returned: %v", nbd.nbd.Name(), err)
	}
	err = ioctl(nbd.nbd.Fd(), ioctlClearSock, 0)
	if err != nil {
		clog.Errorf("error clear socket for %s. ioctl returned: %v", nbd.nbd.Name(), err)
	}
	err = ioctl(nbd.nbd.Fd(), ioctlClearQueue, 0)
	if err != nil {
		clog.Errorf("error clear queue for %s. ioctl returned: %v", nbd.nbd.Name(), err)
	}
	err = ioctl(nbd.nbd.Fd(), ioctlClearQueue, 0)
	if err != nil {
		clog.Errorf("error clear queue for %s. ioctl returned: %v", nbd.nbd.Name(), err)
	}
	err = nbd.nbd.Close()
	if err != nil {
		clog.Errorf("error close nbd device %s. ioctl returned: %v", nbd.nbd.Name(), err)
	}
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
			buf = buf[:16]
		case cmdTrim:
			if err := dev.Trim(hdr.offset(), int64(hdr.length())); err != nil {
				clog.Printf("trim error: %s", err)
			}
			fallthrough
		case cmdFlush:
			if err := dev.Sync(); err != nil {
				clog.Printf("sync error: %s", err)
			}
			hdr.putReplyHeader(buf, 0)
			buf = buf[:16]
		case cmdDisc:
			// FIXME: We're actually supposed to wait for outstanding requests to finish.
			if err := dev.Sync(); err != nil {
				clog.Printf("sync error: %s", err)
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

func (h *reqHeader) handle() uint64 {
	return binary.BigEndian.Uint64(h[8:16])
}

func (h *reqHeader) length() uint32 {
	return binary.BigEndian.Uint32(h[24:28])
}

func (h *reqHeader) magic() uint32 {
	return binary.BigEndian.Uint32(h[0:4])
}

func (h *reqHeader) offset() int64 {
	if u := binary.BigEndian.Uint64(h[16:24]); u <= math.MaxInt64 {
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
