// aoe provides the implementation of an ATA over Ethernet server, backed by a Torus block volume.
package aoe

import (
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/torus/block"

	"github.com/coreos/pkg/capnslog"
	"github.com/mdlayher/aoe"
	"github.com/mdlayher/raw"
	"golang.org/x/net/bpf"
	"golang.org/x/net/context"
)

var (
	clog          = capnslog.NewPackageLogger("github.com/coreos/torus", "aoe")
	broadcastAddr = net.HardwareAddr([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
)

type Server struct {
	dfs *block.BlockVolume

	dev Device

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	major uint16
	minor uint8

	usingBPF bool
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

// NewServer creates a new Server which utilizes the specified block volume.
// If options is nil, DefaultServerOptions will be used.
func NewServer(b *block.BlockVolume, options *ServerOptions) (*Server, error) {
	f, err := b.OpenBlockFile()
	if err != nil {
		return nil, err
	}

	if err := f.Sync(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	fd := &FileDevice{f}

	as := &Server{
		dfs:    b,
		dev:    fd,
		ctx:    ctx,
		cancel: cancel,
		wg:     wg,
		major:  options.Major,
		minor:  options.Minor,
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
			// Must specify our server's major/minor address or else
			// the request will be filtered
			Major:   s.major,
			Minor:   s.minor,
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
			// If user does not have permission to attach a BPF filter to an
			// interface, continue without one
			if !os.IsPermission(err) {
				return err
			}

			clog.Debugf("permission denied, continuing with BPF filter disabled")
		} else {
			clog.Debugf("BPF filter enabled")
			s.usingBPF = true
		}
	}

	clog.Tracef("beginning server loop on %+v", iface)

	// Start goroutine to sync device at regular intervals, and halt when
	// the Server's Close method is called.
	s.wg.Add(1)
	go func() {
		for {
			if err := s.dev.Sync(); err != nil {
				clog.Warningf("failed to sync %s: %v", s.dev, err)
			}

			select {
			case <-time.After(5 * time.Second):
			case <-s.ctx.Done():
				clog.Debugf("stopping device sync goroutine")
				s.wg.Done()
				return
			}
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

		clog.Tracef("recv: %d %s %+v", n, addr, f.Header)
		clog.Tracef("recv arg: %+v", f.Header.Arg)

		s.handleFrame(addr, iface, &f)
	}

	return nil
}

func (s *Server) handleFrame(from net.Addr, iface *Interface, f *Frame) (int, error) {
	hdr := &f.Header

	// If a BPF filter cannot be used, filter requests not bound for
	// this server in userspace.
	if !s.usingBPF {
		// Ignore client requests that are not being broadcast or sent to
		// our major/minor address combination.
		if hdr.Major != aoe.BroadcastMajor && hdr.Major != s.major {
			clog.Debugf("ignoring header with major address %d", hdr.Major)
			return 0, nil
		}
		if hdr.Minor != aoe.BroadcastMinor && hdr.Minor != s.minor {
			clog.Debugf("ignoring header with minor address %d", hdr.Minor)
			return 0, nil
		}
	}

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
		clog.Tracef("cfgarg: %+v", cfgarg)

		switch cfgarg.Command {
		case aoe.ConfigCommandRead:
			hdr.Arg = defaultConfig(iface.MTU)

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
	clog.Debugf("canceling background goroutines")
	s.cancel()
	clog.Debugf("waiting for background goroutines to stop")
	s.wg.Wait()
	return s.dev.Close()
}

// defaultConfig is the default AoE server configuration.
func defaultConfig(mtu int) *aoe.ConfigArg {
	return &aoe.ConfigArg{
		// if < 2, linux aoe handles it poorly
		BufferCount:     2,
		FirmwareVersion: 0,
		// naive, but works.
		SectorCount:  uint8(mtu / 512),
		Version:      aoe.Version,
		Command:      aoe.ConfigCommandRead,
		StringLength: 0,
		String:       []byte{},
	}
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
