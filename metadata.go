package agro

import (
	"fmt"
	"strings"

	"github.com/barakmich/agro/models"
)

type Path struct {
	Volume string
	Path   string
}

func (p Path) IsDir() (b bool) {
	if len(p.Path) == 0 {
		return false
	}

	return p.Path[len(p.Path)-1] == byte('/')
}

func (p Path) GetDepth() int {
	if p.Path == "/" {
		return 0
	}
	return strings.Count(strings.TrimSuffix(p.Path, "/"), "/")
}

func (p Path) Key() string {
	return fmt.Sprintf("%s:%04x:%s", p.Volume, p.GetDepth(), p.Path)
}

func (p Path) SubdirsPrefix() string {
	return fmt.Sprintf("%s:%04x:%s", p.Volume, p.GetDepth()+1, p.Path)
}

type MetadataService interface {
	Mkfs() error
	CreateVolume(volume string) error // TODO(barakmich): Volume and FS options
	GetVolumes() ([]string, error)
	GetVolumeID(volume string) (VolumeID, error)

	CommitInodeIndex() (INodeID, error)

	Mkdir(path Path, dir *models.Directory) error
	Getdir(path Path) (*models.Directory, []Path, error)
	// TODO(barakmich): Get ring, get other nodes, look up nodes for keys, etc.
	// TODO(barakmich): Extend with GC interaction, et al
}

type CreateMetadataFunc func(address string) MetadataService

var metadata map[string]CreateMetadataFunc

func RegisterMetadataProvider(name string, newFunc CreateMetadataFunc) {
	if metadata == nil {
		metadata = make(map[string]CreateMetadataFunc)
	}
	metadata[name] = newFunc
}

func CreateMetadata(name, address string) MetadataService {
	return metadata[name](address)
}
