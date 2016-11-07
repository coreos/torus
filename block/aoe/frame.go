package aoe

import (
	"net"

	"github.com/mdlayher/aoe"
	"github.com/mdlayher/ethernet"
	"github.com/mdlayher/raw"
)

type Frame struct {
	// received ethernet frame
	ethernet.Frame
	// received AoE header
	aoe.Header
}

func (f *Frame) UnmarshalBinary(data []byte) error {
	if err := f.Frame.UnmarshalBinary(data); err != nil {
		return err
	}

	if err := f.Header.UnmarshalBinary(f.Frame.Payload); err != nil {
		return err
	}

	return nil
}

type WriterTo interface {
	WriteTo(b []byte, addr net.Addr) (n int, err error)
}

type FrameSender struct {
	orig *Frame
	dst  net.HardwareAddr
	src  net.HardwareAddr
	conn WriterTo

	major uint16
	minor uint8
}

func (fs *FrameSender) Send(hdr *aoe.Header) (int, error) {
	hdr.Version = 1
	hdr.FlagResponse = true
	hdr.Major = fs.major
	hdr.Minor = fs.minor
	hdr.Tag = fs.orig.Tag

	hbuf, err := hdr.MarshalBinary()
	if err != nil {
		panic(err)
	}

	frame := &ethernet.Frame{
		Destination: fs.dst,
		Source:      fs.src,
		EtherType:   aoe.EtherType,
		Payload:     hbuf,
	}

	ebuf, err := frame.MarshalBinary()
	if err != nil {
		panic(err)
	}

	clog.Tracef("send: %d %s %+v", len(ebuf), fs.dst, hdr)
	clog.Tracef("send arg: %+v", hdr.Arg)

	return fs.conn.WriteTo(ebuf, &raw.Addr{HardwareAddr: fs.dst})
}

func (fs *FrameSender) SendError(aerr aoe.Error) (int, error) {
	hdr := fs.orig.Header
	hdr.FlagError = true
	hdr.Error = aerr

	return fs.Send(&hdr)
}
