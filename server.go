package agro

import (
	"os"

	"github.com/coreos/agro/models"
)

// Server is the interface representing the basic ways to interact with the
// filesystem.
type Server interface {
	// Standard file path calls.
	Create(Path) (File, error)
	Open(Path) (File, error)
	OpenFile(p Path, flag int, perm os.FileMode) (File, error)
	OpenFileMetadata(p Path, flag int, md *models.Metadata) (File, error)
	Rename(p Path, new Path) error
	Link(p Path, new Path) error
	Symlink(p Path, new Path) error
	Lstat(Path) (os.FileInfo, error)
	Readdir(Path) ([]Path, error)
	Remove(Path) error
	Mkdir(Path) error

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
