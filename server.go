package agro

import "io"

// Server is the interface representing the basic ways to interact with the
// filesystem.
type Server interface {
	Close() error

	// Return some global server information, usually from the underlying
	// metadata service.
	Info() (ServerInfo, error)

	// BeginHeartbeat spawns a goroutine for heartbeats. Non-blocking.
	BeginHeartbeat() error

	// ListenReplication opens the internal networking port and connects to the cluster
	ListenReplication(addr string) error
	// OpenReplication connects to the cluster without opening the internal networking.
	OpenReplication() error

	// Write a bunch of debug output to the io.Writer.
	// Implementation specific.
	Debug(io.Writer) error
}

type ServerInfo struct {
	BlockSize uint64
}
