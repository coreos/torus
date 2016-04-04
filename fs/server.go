package fs

import (
	"os"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

type FSServer interface {
	agro.Server

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
}
