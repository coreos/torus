package agro

import (
	"fmt"
	"io"

	"golang.org/x/net/context"

	"github.com/coreos/agro/models"
	"github.com/coreos/pkg/capnslog"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "agro")

type MetadataKind int

const (
	EtcdMetadata MetadataKind = iota
	TempMetadata
)

// MetadataService is the interface representing the basic ways to manipulate
// consistently stored fileystem metadata.
type MetadataService interface {
	GetVolumes() ([]*models.Volume, error)
	GetVolume(volume string) (*models.Volume, error)
	NewVolumeID() (VolumeID, error)
	Kind() MetadataKind

	GlobalMetadata() (GlobalMetadata, error)

	// Returns a UUID based on the underlying datadir. Should be
	// unique for every created datadir.
	UUID() string

	GetRing() (Ring, error)
	SubscribeNewRings(chan Ring)
	UnsubscribeNewRings(chan Ring)
	SetRing(ring Ring) error

	WithContext(ctx context.Context) MetadataService

	GetLease() (int64, error)
	RegisterPeer(lease int64, pi *models.PeerInfo) error
	GetPeers() (PeerInfoList, error)

	Close() error

	CommitINodeIndex(VolumeID) (INodeID, error)
	GetINodeIndex(VolumeID) (INodeID, error)
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
