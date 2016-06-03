package aoe

import (
	"fmt"
	"net"
	"syscall"
	"time"

	"github.com/coreos/torus/block"

	"github.com/coreos/pkg/capnslog"
	"github.com/mdlayher/aoe"
	"github.com/mdlayher/raw"
	"golang.org/x/net/bpf"
)

var (
	clog          = capnslog.NewPackageLogger("github.com/coreos/torus", "aoe")
	broadcastAddr = net.HardwareAddr([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
)

type Server struct {
	dfs *block.BlockVolume

	dev Device

	major uint16
	minor uint8
}

// ServerOptions specifies options for a Server.
type ServerOptions struct {
	// Major and Minor specify the major and minor address of an AoE server.
	// Typically, all AoE devices on a single server will share the same
	// major address, but have different minor addresses.
	//
	// It is important to note that all AoE servers on the same layer 2
	// network must have different major and minor addresses.
	Major uint16
	Minor uint8
}

// DefaultServerOptions is the default ServerOptions configuration used
// by NewServer when none is specified.
var DefaultServerOptions = &ServerOptions{
	Major: 1,
	Minor: 1,
}

// NewServer creates a new Server which utilizes the specified block volume.
// If options is nil, DefaultServerOptions will be used.
func NewServer(b *block.BlockVolume, options *ServerOptions) (*Server, error) {
	if options == nil {
		options = DefaultServerOptions
	}

	f, err := b.OpenBlockFile()
	if err != nil {
		return nil, err
	}

	f.Sync()

	fd := &FileDevice{f}

	as := &Server{
		dfs:   b,
		dev:   fd,
		major: options.Major,
		minor: options.Minor,
	}

	return as, nil
}

func (s *Server) advertise(iface *Interface) error {
	// little hack to trigger a broadcast
	from := &raw.Addr{
		HardwareAddr: broadcastAddr,
	}

	fr := &Frame{
		Header: aoe.Header{
			Command: aoe.CommandQueryConfigInformation,
			Arg: &aoe.ConfigArg{
				Command: aoe.ConfigCommandRead,
			},
		},
	}

	_, err := s.handleFrame(from, iface, fr)
	return err
}

// A bpfPacketConn is a net.PacketConn which can have a BPF program attached
// to it for kernel-level filtering.
type bpfPacketConn interface {
	net.PacketConn
	SetBPF(filter []bpf.RawInstruction) error
}

// raw.Conn is a bpfPacketConn.
var _ bpfPacketConn = &raw.Conn{}

func (s *Server) Serve(iface *Interface) error {
	// If available, attach a BPF filter to net.PacketConn.
	if bp, ok := iface.PacketConn.(bpfPacketConn); ok {
		clog.Debugf("attaching BPF program to %T", bp)
		if err := bp.SetBPF(s.mustAssembleBPF(iface.MTU)); err != nil {
			return err
		}
	}

	clog.Tracef("beginning server loop on %+v", iface)

	// cheap sync proc, should stop when server is shut off
	go func() {
		for {
			if err := s.dev.Sync(); err != nil {
				clog.Warningf("failed to sync %s: %v", s.dev, err)
			}

			time.Sleep(5 * time.Second)
		}
	}()

	// broadcast ourselves
	if err := s.advertise(iface); err != nil {
		clog.Errorf("advertisement failed: %v", err)
		return err
	}

	for {
		payload := make([]byte, iface.MTU)
		n, addr, err := iface.ReadFrom(payload)
		if err != nil {
			clog.Errorf("ReadFrom failed: %v", err)
			// will be syscall.EBADF if the conn from raw closed
			if err == syscall.EBADF {
				break
			}
			return err
		}

		// resize payload
		payload = payload[:n]

		var f Frame
		if err := f.UnmarshalBinary(payload); err != nil {
			clog.Errorf("Failed to unmarshal frame: %v", err)
			continue
		}

		clog.Debugf("recv %d %s %+v", n, addr, f.Header)
		//clog.Debugf("recv arg %+v", f.Header.Arg)

		s.handleFrame(addr, iface, &f)
	}

	return nil
}

func (s *Server) handleFrame(from net.Addr, iface *Interface, f *Frame) (int, error) {
	hdr := &f.Header

	sender := &FrameSender{
		orig:  f,
		dst:   from.(*raw.Addr).HardwareAddr,
		src:   iface.HardwareAddr,
		conn:  iface.PacketConn,
		major: s.major,
		minor: s.minor,
	}

	switch hdr.Command {
	case aoe.CommandIssueATACommand:
		n, err := aoe.ServeATA(sender, hdr, s.dev)
		if err != nil {
			clog.Errorf("ServeATA failed: %v", err)
			switch err {
			case aoe.ErrInvalidATARequest:
				return sender.SendError(aoe.ErrorBadArgumentParameter)
			default:
				return sender.SendError(aoe.ErrorDeviceUnavailable)
			}
		}

		return n, nil
	case aoe.CommandQueryConfigInformation:
		cfgarg := f.Header.Arg.(*aoe.ConfigArg)
		clog.Debugf("cfgarg: %+v", cfgarg)

		switch cfgarg.Command {
		case aoe.ConfigCommandRead:
			hdr.Arg = &aoe.ConfigArg{
				// if < 2, linux aoe handles it poorly
				BufferCount:     2,
				FirmwareVersion: 0,
				// naive, but works.
				SectorCount:  uint8(iface.MTU / 512),
				Version:      1,
				Command:      aoe.ConfigCommandRead,
				StringLength: 0,
				String:       []byte{},
			}

			return sender.Send(hdr)
		}

		return sender.SendError(aoe.ErrorUnrecognizedCommandCode)
	case aoe.CommandMACMaskList:
		fallthrough
	case aoe.CommandReserveRelease:
		fallthrough
	default:
		return sender.SendError(aoe.ErrorUnrecognizedCommandCode)
	}
}

func (s *Server) Close() error {
	return s.dev.Close()
}

// mustAssembleBPF assembles a BPF program to filter out packets not bound
// for this server.
func (s *Server) mustAssembleBPF(mtu int) []bpf.RawInstruction {
	// This BPF program filters out packets that are not bound for this server
	// by checking against both the AoE broadcast addresses and this server's
	// major/minor address combination.  The structure of the incoming ethernet
	// frame and AoE header is as follows:
	//
	// Offset | Length | Comment
	// -------------------------
	//   00   |   06   | Ethernet destination MAC address
	//   06   |   06   | Ethernet source MAC address
	//   12   |   02   | Ethernet EtherType
	// -------------------------
	//   14   |   01   | AoE version + flags
	//   15   |   01   | AoE error
	//   16   |   02   | AoE major address
	//   18   |   01   | AoE minor address
	//
	// Thus, our BPF program needs to check for:
	//  - major address: offset 16, length 2
	//  - minor address: offset 18, length 1
	const (
		majorOffset = 16
		majorLen    = 2
		minorOffset = 18
		minorLen    = 1
	)

	// TODO(mdlayher): this routine likely belongs in package AoE, once the server
	// component is more complete.

	prog, err := bpf.Assemble([]bpf.Instruction{
		// Load major address value from AoE header
		bpf.LoadAbsolute{
			Off:  majorOffset,
			Size: majorLen,
		},
		// If major address is equal to broadcast address, jump to minor address
		// filtering
		bpf.JumpIf{
			Cond:     bpf.JumpEqual,
			Val:      uint32(aoe.BroadcastMajor),
			SkipTrue: 2,
		},
		// If major address is equal to our server's, jump to minor address
		// filtering
		bpf.JumpIf{
			Cond:     bpf.JumpEqual,
			Val:      uint32(s.major),
			SkipTrue: 1,
		},
		// Major address is not our server's or broadcast address
		bpf.RetConstant{
			Val: 0,
		},
		// Load minor address value from AoE header
		bpf.LoadAbsolute{
			Off:  minorOffset,
			Size: minorLen,
		},
		// If minor address is equal to broadcast address, jump to accept packet
		bpf.JumpIf{
			Cond:     bpf.JumpEqual,
			Val:      uint32(aoe.BroadcastMinor),
			SkipTrue: 2,
		},
		// If minor address is equal to our server's, jump to accept packet
		bpf.JumpIf{
			Cond:     bpf.JumpEqual,
			Val:      uint32(s.minor),
			SkipTrue: 1,
		},
		// Minor address is not our server's or broadcast address
		bpf.RetConstant{
			Val: 0,
		},
		// Accept the packet bytes up to the interface's MTU
		bpf.RetConstant{
			Val: uint32(mtu),
		},
	})
	if err != nil {
		panic(fmt.Sprintf("failed to assemble BPF program: %v", err))
	}

	return prog
}
