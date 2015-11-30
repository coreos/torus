package agro

import (
	"fmt"
	"path"
	"strings"

	"golang.org/x/net/context"

	"github.com/barakmich/agro/models"
	"github.com/coreos/pkg/capnslog"
)

var clog = capnslog.NewPackageLogger("github.com/barakmich/agro", "agro")

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

func (p Path) Super() (Path, bool) {
	if !p.IsDir() {
		return p, false
	}
	super, _ := path.Split(strings.TrimSuffix(p.Path, "/"))
	return Path{
		Volume: p.Volume,
		Path:   super,
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

// MetadataService is the interface representing the basic ways to manipulate
// consistently stored fileystem metadata.
type MetadataService interface {
	CreateVolume(volume string) error // TODO(barakmich): Volume and FS options
	GetVolumes() ([]string, error)
	GetVolumeID(volume string) (VolumeID, error)

	//TODO(barakmich): Relieve contention by making this per-volume
	CommitInodeIndex() (INodeID, error)

	Mkdir(path Path, dir *models.Directory) error
	Getdir(path Path) (*models.Directory, []Path, error)
	SetFileINode(path Path, ref INodeRef) error

	GlobalMetadata() (GlobalMetadata, error)

	// Returns a UUID based on the underlying datadir. Should be
	// unique for every created datadir.
	UUID() string

	RegisterPeer(*models.PeerInfo) error
	GetPeers() ([]*models.PeerInfo, error)

	WithContext(ctx context.Context) MetadataService

	// TODO(barakmich): Extend with GC interaction, et al
	Close() error
}

type GlobalMetadata struct {
	BlockSize        uint64
	DefaultBlockSpec BlockLayerSpec
}

// CreateMetadataServiceFunc is the signature of a constructor used to create
// a registered MetadataService.
type CreateMetadataServiceFunc func(cfg Config) (MetadataService, error)

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
func CreateMetadataService(name string, cfg Config) (MetadataService, error) {
	clog.Infof("creating metadata service: %s", name)
	return metadataServices[name](cfg)
}

// MkfsFunc is the signature of a function which preformats a metadata service.
type MkfsFunc func(cfg Config, gmd GlobalMetadata) error

var mkfsFuncs map[string]MkfsFunc

// RegisterMkfs is the hook used for implementions of
// MetadataServices to register their ways of creating base metadata to the system.
func RegisterMkfs(name string, newFunc MkfsFunc) {
	if mkfsFuncs == nil {
		mkfsFuncs = make(map[string]MkfsFunc)
	}

	if _, ok := mkfsFuncs[name]; ok {
		panic("agro: attempted to register MkfsFunc " + name + " twice")
	}

	mkfsFuncs[name] = newFunc
}

// Mkfs calls the specific Mkfs function provided by a metadata package.
func Mkfs(name string, cfg Config, gmd GlobalMetadata) error {
	clog.Debugf("running mkfs for service type: %s", name)
	return mkfsFuncs[name](cfg, gmd)
}
