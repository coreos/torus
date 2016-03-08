package agro

import (
	"io"
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
	Symlink(to string, new Path) error
	Lstat(Path) (os.FileInfo, error)
	Readdir(Path) ([]Path, error)
	Remove(Path) error
	Mkdir(Path, *models.Metadata) error

	Chmod(name Path, mode os.FileMode) error
	Chown(name Path, uid, gid int) error
	// Some server metacalls.
	CreateFSVolume(string) error
	GetVolumes() ([]*models.Volume, error)

	Close() error

	Statfs() (ServerStats, error)

	// BeginHeartbeat spawns a goroutine for heartbeats. Non-blocking.
	BeginHeartbeat() error

	// ListenReplication opens the internal networking port and connects to the cluster
	ListenReplication(addr string) error
	// OpenReplication connects to the cluster without opening the internal networking.
	OpenReplication() error
	Debug(io.Writer) error
}

type ServerStats struct {
	BlockSize uint64
}
