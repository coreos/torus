package aoe

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/coreos/torus/block"

	"github.com/mdlayher/aoe"
)

var (
	_ Device = &FileDevice{}
)

type Device interface {
	io.ReadWriteSeeker
	io.ReaderAt
	io.WriterAt
	Sync() error
	Close() error
	aoe.Identifier
}

// note: ATA 'words' are 16 bits, so all byte offsets are multiplied
// by 2.

// encode a 28 bit LBA
func lba28(s int64) [4]byte {
	l := [4]byte{}

	l[0] = byte(s)
	s = s >> 8
	l[1] = byte(s)
	s = s >> 8
	l[2] = byte(s)
	s = s >> 8
	l[3] = byte(s) & 0xf

	return l
}

// encode a 48 bit LBA
func lba48(s int64) [6]byte {
	l := [6]byte{}

	l[0] = byte(s)
	s = s >> 8
	l[1] = byte(s)
	s = s >> 8
	l[2] = byte(s)
	s = s >> 8
	l[3] = byte(s)
	s = s >> 8
	l[4] = byte(s)
	s = s >> 8
	l[5] = byte(s)

	return l
}

// poke a short into p at off
func pshort(p []byte, off int, v uint16) {
	off = off * 2
	binary.LittleEndian.PutUint16(p[off:off+2], v)
}

// poke a string into p at off and pad with space
func pstring(p []byte, off int, sz int, ident string) {
	if sz%2 != 0 {
		panic("can't encode odd length string")
	}

	id := make([]byte, sz)
	for i := 0; i < len(id); i++ {
		id[i] = byte(' ')
	}
	copy(id, []byte(ident))
	for i := 0; i < len(id); i += 2 {
		id[i], id[i+1] = id[i+1], id[i]
	}

	copy(p[off*2:], id)
}

type FileDevice struct {
	*block.BlockFile
}

func (fd *FileDevice) Sectors() (int64, error) {
	fi := fd.BlockFile.Size()
	if fi == 0 {
		return 0, errors.New("empty file device?")
	}

	return int64(fi) / 512, nil
}

func (fd *FileDevice) Identify() ([512]byte, error) {
	bufa := [512]byte{}

	sectors, err := fd.Sectors()
	if err != nil {
		return bufa, err
	}

	buf := bufa[:]

	pshort(buf, 47, 0x8000)
	pshort(buf, 49, 0x0200)
	pshort(buf, 50, 0x4000)

	// PIO mode 4
	pshort(buf, 53, 0x0002)
	pshort(buf, 64, 0x0002)

	// claim ATA8-ACS support
	pshort(buf, 80, 0x00F0)

	pshort(buf, 83, 0x5400)
	pshort(buf, 84, 0x4000)
	pshort(buf, 86, 0x1400)
	pshort(buf, 87, 0x4000)
	pshort(buf, 93, 0x400b)

	// we support DRAT
	pshort(buf, 69, 0x4000)
	// we support TRIM
	pshort(buf, 169, 0x0001)

	// Serial number
	pstring(buf, 10, 20, "0")

	// Firmware revision
	pstring(buf, 23, 8, "V0")

	// Model number
	pstring(buf, 27, 40, "torus AoE")

	l28 := lba28(sectors)
	// 28-bit LBA sectors
	copy(buf[60*2:], l28[:])
	l48 := lba48(sectors)
	// 48-bit LBA sectors
	copy(buf[100*2:], l48[:])

	return bufa, nil
}
