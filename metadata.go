package agro

import (
	"fmt"
	"strings"

	"github.com/barakmich/agro/types"
)

type Path struct {
	Volume string
	Path   string
}

func (p Path) GetDepth() int {
	if p.Path == "/" {
		return 0
	}
	a := strings.TrimSuffix(p.Path, "/")
	return len(strings.Split(a, "/")) - 1
}

func (p Path) Key() string {
	return fmt.Sprintf("%s:%4x:%s", p.Volume, p.GetDepth(), p.Path)
}

type Metadata interface {
	Mkfs()
	CreateVolume(volume string) // TODO(barakmich): Volume and FS options

	CommitInodeIndex() uint64

	Mkdir(path Path, dir *types.Directory)
	Getdir(path Path) (*types.Directory, []Path)
	// TODO(barakmich): Get ring, get other nodes, look up nodes for keys, etc.
	// TODO(barakmich): Extend with GC interaction, et al
}
