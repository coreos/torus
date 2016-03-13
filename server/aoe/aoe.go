package aoe

import (
	"net"
	"syscall"

	"github.com/coreos/agro"

	"github.com/coreos/pkg/capnslog"
	"github.com/mdlayher/aoe"
	"github.com/mdlayher/raw"
)

var (
	clog = capnslog.NewPackageLogger("github.com/coreos/agro", "aoe")
)

type Server struct {
	dfs agro.BlockServer

	dev Device

	major      uint16
	minor      uint8
	configData []byte
}

func NewServer(srv agro.BlockServer, volume string) (*Server, error) {
	f, err := srv.OpenBlockFile(volume)
	if err != nil {
		return nil, err
	}

	f.Sync()

	fd := &FileDevice{f}

	as := &Server{
		dfs:        srv,
		dev:        fd,
		major:      1,
		minor:      1,
		configData: make([]byte, 0),
	}

	return as, nil
}

func (s *Server) Serve(iface *Interface) error {
	clog.Tracef("beginning server loop on %+v", iface)

	// TODO(mischief): broadcast when online

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
				BufferCount:     0,
				FirmwareVersion: 0,
				// naive, but works.
				SectorCount:  uint8(iface.MTU / 512),
				Version:      1,
				Command:      aoe.ConfigCommandRead,
				StringLength: uint16(len(s.configData)),
				String:       s.configData,
			}

			return sender.Send(hdr)
		case aoe.ConfigCommandSet:
			if len(s.configData) == 0 {
				clog.Debugf("setting data: %s", string(cfgarg.String))
				s.configData = cfgarg.String
			}
			hdr.Arg = &aoe.ConfigArg{
				BufferCount:     0,
				FirmwareVersion: 0,
				// naive, but works.
				SectorCount:  uint8(iface.MTU / 512),
				Version:      1,
				Command:      aoe.ConfigCommandSet,
				StringLength: uint16(len(s.configData)),
				String:       s.configData,
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
