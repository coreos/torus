package agro

import (
	"fmt"
	"strings"

	"github.com/barakmich/agro/models"
)

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

// GetDepth returns the distance of the current path from the root of the
// filesystem.
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

// MetadataService is the interface representing the basic ways to manipulate
// consistently stored fileystem metadata.
type MetadataService interface {
	Mkfs(GlobalMetadata) error
	CreateVolume(volume string) error // TODO(barakmich): Volume and FS options
	GetVolumes() ([]string, error)
	GetVolumeID(volume string) (VolumeID, error)

	CommitInodeIndex() (INodeID, error)

	Mkdir(path Path, dir *models.Directory) error
	Getdir(path Path) (*models.Directory, []Path, error)

	GlobalMetadata() (GlobalMetadata, error)
	// TODO(barakmich): Get ring, get other nodes, look up nodes for keys, etc.
	// TODO(barakmich): Extend with GC interaction, et al
}

type GlobalMetadata struct {
	BlockSize        uint64
	DefaultBlockSpec BlockLayerSpec
}

// CreateMetadataServiceFunc is the signature of a constructor used to create
// a registered MetadataService.
type CreateMetadataServiceFunc func(address string) MetadataService

var metadataServices map[string]CreateMetadataServiceFunc

// RegisterMetadataService is the hook used for implementions of
// MetadataServices to register themselves to the system. This is usually
// called in the init() of the package that implements the MetadataService.
// A similar pattern is used in database/sql of the standard library.
func RegisterMetadataService(name string, newFunc CreateMetadataServiceFunc) {
	if metadataServices == nil {
		metadataServices = make(map[string]CreateMetadataServiceFunc)
	}

	if _, ok := metadataServices[name]; ok {
		panic("agro: attempted to register MetadataService " + name + " twice")
	}

	metadataServices[name] = newFunc
}

// CreateMetadataService calls the constructor of the specified MetadataService
// with the provided address.
func CreateMetadataService(name, address string) MetadataService {
	return metadataServices[name](address)
}
