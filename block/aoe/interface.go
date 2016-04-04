package aoe

import (
	"net"

	"github.com/mdlayher/raw"
)

// implements net.PacketConn
type Interface struct {
	*net.Interface
	net.PacketConn
}

func NewInterface(ifname string) (*Interface, error) {
	ifc, err := net.InterfaceByName(ifname)
	if err != nil {
		return nil, err
	}

	pc, err := raw.ListenPacket(ifc, raw.ProtocolAoE)
	if err != nil {
		return nil, err
	}

	ai := &Interface{ifc, pc}
	return ai, nil
}
