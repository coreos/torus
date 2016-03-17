package agro

import (
	"fmt"
	"io"
	"path"
	"strings"

	"golang.org/x/net/context"

	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/agro/models"
	"github.com/coreos/pkg/capnslog"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "agro")

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

// MetadataService is the interface representing the basic ways to manipulate
// consistently stored fileystem metadata.
type MetadataService interface {
	CreateVolume(*models.Volume) error
	GetVolumes() ([]*models.Volume, error)
	GetVolume(volume string) (*models.Volume, error)

	CommitINodeIndex(volume string) (INodeID, error)
	GetINodeIndex(volume string) (INodeID, error)
	GetINodeIndexes() (map[string]INodeID, error)

	GlobalMetadata() (GlobalMetadata, error)

	// Returns a UUID based on the underlying datadir. Should be
	// unique for every created datadir.
	UUID() string

	GetPeers() (PeerInfoList, error)

	GetRing() (Ring, error)
	SubscribeNewRings(chan Ring)
	UnsubscribeNewRings(chan Ring)
	SetRing(ring Ring) error

	WithContext(ctx context.Context) MetadataService

	GetLease() (int64, error)
	RegisterPeer(lease int64, pi *models.PeerInfo) error

	Close() error
}

type BlockMetadataService interface {
	MetadataService
	LockBlockVolume(lease int64, vid VolumeID) error
	GetBlockVolumeINode(vid VolumeID) (INodeRef, error)
	SyncBlockVolume(INodeRef) error
	UnlockBlockVolume(vid VolumeID) error
}

type FSMetadataService interface {
	MetadataService

	Mkdir(path Path, dir *models.Metadata) error
	ChangeDirMetadata(path Path, dir *models.Metadata) error
	Getdir(path Path) (*models.Directory, []Path, error)
	Rmdir(path Path) error
	SetFileEntry(path Path, ent *models.FileEntry) error

	GetINodeChains(vid VolumeID) ([]*models.FileChainSet, error)
	GetChainINode(base INodeRef) (INodeRef, error)
	SetChainINode(base INodeRef, was INodeRef, new INodeRef) error

	ClaimVolumeINodes(lease int64, vol VolumeID, inodes *roaring.Bitmap) error

	ModifyDeadMap(vol VolumeID, live *roaring.Bitmap, dead *roaring.Bitmap) error
	GetVolumeLiveness(vol VolumeID) (*roaring.Bitmap, []*roaring.Bitmap, error)
}

type DebugMetadataService interface {
	DumpMetadata(io.Writer) error
}

type GlobalMetadata struct {
	BlockSize        uint64
	DefaultBlockSpec BlockLayerSpec
	INodeReplication int
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

	if mdsf, ok := metadataServices[name]; ok {
		return mdsf(cfg)
	}

	return nil, fmt.Errorf("agro: the metadata service %q doesn't exist", name)
}

// MkfsFunc is the signature of a function which preformats a metadata service.
type MkfsFunc func(cfg Config, gmd GlobalMetadata, ringType RingType) error

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
func Mkfs(name string, cfg Config, gmd GlobalMetadata, ringType RingType) error {
	clog.Debugf("running mkfs for service type: %s", name)
	return mkfsFuncs[name](cfg, gmd, ringType)
}

type SetRingFunc func(cfg Config, r Ring) error

var setRingFuncs map[string]SetRingFunc

// RegisterSetRing is the hook used for implementions of MetadataServices to
// register their ways of creating base metadata to the system.
func RegisterSetRing(name string, newFunc SetRingFunc) {
	if setRingFuncs == nil {
		setRingFuncs = make(map[string]SetRingFunc)
	}

	if _, ok := setRingFuncs[name]; ok {
		panic("agro: attempted to register SetRingFunc " + name + " twice")
	}

	setRingFuncs[name] = newFunc
}

// SetRing calls the specific SetRing function provided by a metadata package.
func SetRing(name string, cfg Config, r Ring) error {
	clog.Debugf("running setRing for service type: %s", name)
	return setRingFuncs[name](cfg, r)
}
