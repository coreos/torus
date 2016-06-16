package aoe

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/coreos/torus"
	"github.com/coreos/torus/block"
	"github.com/coreos/torus/metadata/temp"
	// Imported so that storage registers properly
	_ "github.com/coreos/torus/storage"

	"github.com/mdlayher/aoe"
	"github.com/mdlayher/bpftest"
	"github.com/mdlayher/ethernet"
	"github.com/mdlayher/raw"
	"golang.org/x/net/bpf"
)

const (
	testMTU = 9000
)

func TestServeCommandQueryConfigInformation(t *testing.T) {
	// Request that the server send its current configuration
	req := &aoe.Header{
		Version: aoe.Version,
		Major:   1,
		Minor:   1,
		Command: aoe.CommandQueryConfigInformation,
		Tag:     [4]byte{0xde, 0xad, 0xbe, 0xef},
		Arg: &aoe.ConfigArg{
			Command: aoe.ConfigCommandRead,
		},
	}

	res := testRequest(t, req)
	arg, ok := res.Arg.(*aoe.ConfigArg)
	if !ok {
		t.Fatalf("incorrect argument type for configuration request: %T", res.Arg)
	}

	// Verify the server is using the default configuration on startup
	if want, got := *req, *res; !headersEqual(got, want) {
		t.Fatalf("unexpected AoE header:\n - want: %+v\n-  got: %+v", want, got)
	}

	wantArg := defaultConfig(testMTU)
	if want, got := wantArg, arg; !reflect.DeepEqual(want, got) {
		t.Fatalf("unexpected AoE config arg:\n - want: %+v\n-  got: %+v", want, got)
	}
}

func TestServeCommandMACMaskList(t *testing.T) {
	// Request that the server send its current MAC mask list
	req := &aoe.Header{
		Version: aoe.Version,
		Major:   1,
		Minor:   1,
		Command: aoe.CommandMACMaskList,
		Tag:     [4]byte{0xde, 0xad, 0xbe, 0xef},
		Arg: &aoe.MACMaskArg{
			Command: aoe.MACMaskCommandRead,
		},
	}

	res := testRequest(t, req)

	// At this time, MAC mask lists are not implemented in the AoE server.
	err := *req
	err.FlagResponse = true
	err.FlagError = true
	err.Error = aoe.ErrorUnrecognizedCommandCode

	if want, got := err, *res; !headersEqual(got, want) {
		t.Fatalf("unexpected AoE header:\n - want: %+v\n-  got: %+v", want, got)
	}
}

func TestBPFProgramWrongMajorAddress(t *testing.T) {
	s := &Server{
		major: 1,
		minor: 1,
	}

	h := &aoe.Header{
		Major: 2,
		Minor: 1,
	}

	if testBPFProgram(t, s, h) {
		t.Fatal("BPF program should have filtered incorrect AoE major address")
	}
}

func TestBPFProgramWrongMinorAddress(t *testing.T) {
	s := &Server{
		major: 1,
		minor: 1,
	}

	h := &aoe.Header{
		Major: 1,
		Minor: 2,
	}

	if testBPFProgram(t, s, h) {
		t.Fatal("BPF program should have filtered incorrect AoE minor address")
	}
}

func TestBPFProgramOK(t *testing.T) {
	s := &Server{
		major: 2,
		minor: 2,
	}

	h := &aoe.Header{
		Major: 2,
		Minor: 2,
	}

	if !testBPFProgram(t, s, h) {
		t.Fatal("BPF program should not have filtered correct AoE header")
	}
}

func TestBPFProgramBroadcastOK(t *testing.T) {
	s := &Server{
		major: 1,
		minor: 1,
	}

	h := &aoe.Header{
		Major: aoe.BroadcastMajor,
		Minor: aoe.BroadcastMinor,
	}

	if !testBPFProgram(t, s, h) {
		t.Fatal("BPF program should not have filtered correct AoE broadcast header")
	}
}

// testBPFProgram builds a BPF program for the input server and compares the
// output of the program against the input AoE header, returning whether or
// not the header was accepted by the filter.
func testBPFProgram(t *testing.T, s *Server, h *aoe.Header) bool {
	filter, ok := bpf.Disassemble(s.mustAssembleBPF(testMTU))
	if !ok {
		t.Fatal("failed to decode all BPF instructions")
	}

	vm, err := bpftest.New(filter)
	if !ok {
		t.Fatalf("failed to load BPF program: %v", err)
	}

	// Fill in empty AoE header fields not relevant to this test
	h.Version = aoe.Version
	h.Command = aoe.CommandQueryConfigInformation
	h.Arg = &aoe.ConfigArg{
		Command: aoe.ConfigCommandRead,
	}

	hb, err := h.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal AoE header to binary: %v", err)
	}

	f := &ethernet.Frame{
		EtherType: aoe.EtherType,
		Payload:   hb,
	}
	fb, err := f.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal Ethernet frame to binary: %v", err)
	}

	_, ok, err = vm.Run(fb)
	if err != nil {
		t.Fatalf("failed to run BPF program: %v", err)
	}

	return ok
}

// testRequest performs a single AoE request using the input request
// header and returns a single AoE response.
func testRequest(t *testing.T, req *aoe.Header) *aoe.Header {
	recv, send, run, done := testAoEServer(t)
	defer done()

	reqb, err := req.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal request AoE header: %v", err)
	}

	reqf := &ethernet.Frame{
		Payload: reqb,
	}

	reqfb, err := reqf.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal request Ethernet frame: %v", err)
	}

	if _, err := send.Write(reqfb); err != nil {
		t.Fatalf("failed to write request to server: %v", err)
	}

	if err := run(); err != nil {
		t.Fatalf("failed to run server: %v", err)
	}

	b, err := ioutil.ReadAll(recv)
	if err != nil {
		t.Fatalf("failed to read server response: %v", err)
	}

	resf := new(ethernet.Frame)
	if err := resf.UnmarshalBinary(b); err != nil {
		t.Fatalf("failed to unmarshal response Ethernet frame: %v", err)
	}

	resh := new(aoe.Header)
	if err := resh.UnmarshalBinary(resf.Payload); err != nil {
		t.Fatalf("failed to unmarshal response AoE header: %v", err)
	}

	return resh
}

// testAoEServer sets up a Torus AoE server and returns:
//  - an io.Reader which contains the server's response to a request
//  - an io.Writer which can be used to issue a request to the server
//  - a closure which can be used to run the server for a single
//    request/response cycle
//  - a closure which can be used to tear down all server resources
func testAoEServer(t *testing.T) (response io.Reader, request io.Writer, run func() error, done func()) {
	ts := torus.NewMemoryServer()

	mds, err := temp.NewTemp(ts.Cfg)
	if err != nil {
		t.Fatalf("failed to configure metadata service: %v", err)
	}
	ts.MDS = mds

	const volName = "test"
	if err := block.CreateBlockVolume(mds, volName, 1024); err != nil {
		t.Fatalf("failed to create block volume: %v", err)
	}

	vol, err := block.OpenBlockVolume(ts, volName)
	if err != nil {
		t.Fatalf("error opening block volume: %v", err)
	}

	as, err := NewServer(vol, nil)
	if err != nil {
		t.Fatalf("error opening AoE server: %v", err)
	}

	serverWrite := bytes.NewBuffer(nil)
	serverRead := bytes.NewBuffer(nil)

	bp := &bufPacketConn{
		rb: serverWrite,
		wb: serverRead,
	}

	iface := &Interface{
		Interface: &net.Interface{
			Name: "lo",
			MTU:  testMTU,
		},
		PacketConn: bp,
	}

	run = func() error {
		if err := as.Serve(iface); err != nil {
			return fmt.Errorf("failed to serve AoE: %v", err)
		}

		bp.mu.Lock()
		defer bp.mu.Unlock()

		if !bp.gotBroadcast {
			return errors.New("broadcast advertise was never received")
		}

		return nil
	}

	done = func() {
		_ = as.Close()
		_ = iface.PacketConn.Close()
		_ = ts.Close()
	}

	return serverRead, serverWrite, run, done
}

// headersEqual compares a response and request AoE header for equality, but
// does not factor in their Arg fields.
func headersEqual(res, req aoe.Header) bool {
	if !res.FlagResponse {
		panic("response AoE header did not have FlagResponse set, please verify parameter order")
	}

	// Set request's response flag for comparison
	req.FlagResponse = true

	// Clear arguments of both request and response so we can compare the
	// headers, and because the arguments are compared separately
	req.Arg = nil
	res.Arg = nil

	return reflect.DeepEqual(res, req)
}

// A bufPacketConn is a net.PacketConn which can inject data into a server's
// net.PacketConn before it starts, and capture the responses the server
// sends back.
type bufPacketConn struct {
	mu sync.Mutex
	rb *bytes.Buffer
	wb *bytes.Buffer

	gotBroadcast bool
	count        int

	noopPacketConn
}

var rawAddr = &raw.Addr{
	HardwareAddr: net.HardwareAddr{0xde, 0xad, 0xbe, 0xef, 0xde, 0xad},
}

func (p *bufPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	p.mu.Lock()
	defer func() { p.count++ }()
	defer p.mu.Unlock()

	// Only read a single ethernet frame before returning EBADF, causing
	// the server to stop its listening loop
	if p.count < 1 {
		n, err := p.rb.Read(b)
		return n, rawAddr, err
	}

	return 0, nil, syscall.EBADF
}

func (p *bufPacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	// Don't want to pass broadcast advertisement back to tests, but we do want
	// to confirm that we receive it
	if bytes.Equal(addr.(*raw.Addr).HardwareAddr, ethernet.Broadcast) {
		p.gotBroadcast = true
		return 0, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	n, err := p.wb.Write(b)
	return int(n), err
}

var _ net.PacketConn = &noopPacketConn{}

// A noopPacketConn is a net.PacketConn which can be embedded in other
// net.PacketConn implementations to reduce code duplication.
type noopPacketConn struct{}

func (noopPacketConn) ReadFrom(b []byte) (int, net.Addr, error)     { return 0, nil, nil }
func (noopPacketConn) WriteTo(b []byte, addr net.Addr) (int, error) { return 0, nil }
func (noopPacketConn) Close() error                                 { return nil }
func (noopPacketConn) LocalAddr() net.Addr                          { return nil }
func (noopPacketConn) SetDeadline(t time.Time) error                { return nil }
func (noopPacketConn) SetReadDeadline(t time.Time) error            { return nil }
func (noopPacketConn) SetWriteDeadline(t time.Time) error           { return nil }
