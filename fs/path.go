package fs

import (
	"fmt"
	"path"
	"strings"

	"github.com/coreos/pkg/capnslog"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "fs")

// Path represents the location of a File including the Volume and the
// unix-style path string.
type Path struct {
	Volume string
	Path   string
}

// IsDir returns whether or not a path is a directory.
func (p Path) IsDir() (b bool) {
	if len(p.Path) == 0 {
		return false
	}

	return p.Path[len(p.Path)-1] == byte('/')
}

// Parent returns the parent directory of the directory provided. False is
// returned if the path provided is not a directory.
func (p Path) Parent() (Path, bool) {
	if !p.IsDir() {
		return p, false
	}

	super, _ := path.Split(strings.TrimSuffix(p.Path, "/"))
	return Path{
		Volume: p.Volume,
		Path:   super,
	}, true
}

// Child appends the filename to the path provided and returns the resulting
// path. False is returned if the path provided is not a directory.
func (p Path) Child(filename string) (Path, bool) {
	if !p.IsDir() {
		return p, false
	}

	return Path{
		Volume: p.Volume,
		Path:   p.Path + filename,
	}, true
}

// GetDepth returns the distance of the current path from the root of the
// filesystem.
func (p Path) GetDepth() int {
	dir, _ := path.Split(p.Path)
	if dir == "/" {
		return 0
	}
	return strings.Count(strings.TrimSuffix(dir, "/"), "/")
}

func (p Path) Key() string {
	dir, _ := path.Split(p.Path)
	return fmt.Sprintf("%s:%04x:%s", p.Volume, p.GetDepth(), dir)
}

func (p Path) SubdirsPrefix() string {
	dir, _ := path.Split(p.Path)
	return fmt.Sprintf("%s:%04x:%s", p.Volume, p.GetDepth()+1, dir)
}

func (p Path) Filename() string {
	_, f := path.Split(p.Path)
	return f
}

func (p Path) Base() string {
	return path.Base(p.Path)
}

func (p Path) Equals(b Path) bool {
	return p.Volume == b.Volume && p.Path == b.Path
}
