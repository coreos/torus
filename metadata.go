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
	return strings.Count(strings.TrimSuffix(p.Path, "/"), "/")
}

func (p Path) Key() string {
	return fmt.Sprintf("%s:%4x:%s", p.Volume, p.GetDepth(), p.Path)
}

type Metadata interface {
	Mkfs() error
	CreateVolume(volume string) error // TODO(barakmich): Volume and FS options
	GetVolumes() ([]string, error)

	CommitInodeIndex() (uint64, error)

	Mkdir(path Path, dir *types.Directory) error
	Getdir(path Path) (*types.Directory, []Path, error)
	// TODO(barakmich): Get ring, get other nodes, look up nodes for keys, etc.
	// TODO(barakmich): Extend with GC interaction, et al
}

type CreateMetadataFunc func(address string) Metadata

var metadata map[string]CreateMetadataFunc

func RegisterMetadataProvider(name string, newFunc CreateMetadataFunc) {
	if metadata == nil {
		metadata = make(map[string]CreateMetadataFunc)
	}
	metadata[name] = newFunc
}

func CreateMetadata(name, address string) Metadata {
	return metadata[name](address)
}
