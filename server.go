package agro

import (
	"os"

	"github.com/barakmich/agro/models"
)

// Server is the interface representing the basic ways to interact with the
// filesystem.
type Server interface {
	// Standard file path calls.
	Create(Path, models.Metadata) (File, error)
	Open(Path) (File, error)
	Lstat(Path) (os.FileInfo, error)

	// Some server metacalls.
	CreateVolume(string) error
	GetVolumes() ([]string, error)

	Close() error

	// BeginHeartbeat spawns a goroutine for heartbeats. Non-blocking.
	BeginHeartbeat() error

	// ListenReplication opens the internal networking port and connects to the cluster
	ListenReplication(addr string) error
	// OpenReplication connects to the cluster without opening the internal networking.
	OpenReplication() error
}
