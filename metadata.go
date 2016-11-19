package torus

import (
	"fmt"
	"io"

	"golang.org/x/net/context"

	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/torus/models"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/torus", "torus")

type MetadataKind int

const (
	EtcdMetadata MetadataKind = iota
	TempMetadata
)

// MetadataService is the interface representing the basic ways to manipulate
// consistently stored fileystem metadata.
type MetadataService interface {
	GetVolumes() ([]*models.Volume, VolumeID, error)
	GetVolume(volume string) (*models.Volume, error)
	NewVolumeID() (VolumeID, error)
	Kind() MetadataKind

	// GlobalMetadata backing struct must be instantiated upon the
	// service creation. If it can not be created the MetadataService
	// creation must fail.
	GlobalMetadata() GlobalMetadata

	// Returns a UUID based on the underlying datadir. Should be
	// unique for every created datadir.
	UUID() string

	GetRing() (Ring, error)
	SubscribeNewRings(chan Ring)
	UnsubscribeNewRings(chan Ring)
	SetRing(ring Ring) error

	WithContext(ctx context.Context) MetadataService

	GetLease() (int64, error)
	RenewLease(int64) error
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
}

// CreateMetadataServiceFunc is the signature of a constructor used to create
// a registered MetadataService.
type CreateMetadataServiceFunc func(cfg Config) (MetadataService, error)

var metadataServices map[string]CreateMetadataServiceFunc

// RegisterMetadataService is the hook used for implementations of
// MetadataServices to register themselves to the system. This is usually
// called in the init() of the package that implements the MetadataService.
// A similar pattern is used in database/sql of the standard library.
func RegisterMetadataService(name string, newFunc CreateMetadataServiceFunc) {
	if metadataServices == nil {
		metadataServices = make(map[string]CreateMetadataServiceFunc)
	}

	if _, ok := metadataServices[name]; ok {
		panic("torus: attempted to register MetadataService " + name + " twice")
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

	return nil, fmt.Errorf("torus: the metadata service %q doesn't exist", name)
}

// InitMDSFunc is the signature of a function which preformats a metadata service.
type InitMDSFunc func(cfg Config, gmd GlobalMetadata, ringType RingType) error

var initMDSFuncs map[string]InitMDSFunc

// RegisterMetadataInit is the hook used for implementations of
// MetadataServices to register their ways of creating base metadata to the system.
func RegisterMetadataInit(name string, newFunc InitMDSFunc) {
	if initMDSFuncs == nil {
		initMDSFuncs = make(map[string]InitMDSFunc)
	}

	if _, ok := initMDSFuncs[name]; ok {
		panic("torus: attempted to register InitMDSFunc " + name + " twice")
	}

	initMDSFuncs[name] = newFunc
}

// InitMDS calls the specific init function provided by a metadata package.
func InitMDS(name string, cfg Config, gmd GlobalMetadata, ringType RingType) error {
	clog.Debugf("running InitMDS for service type: %s", name)
	return initMDSFuncs[name](cfg, gmd, ringType)
}

type WipeMDSFunc func(cfg Config) error

var wipeMDSFuncs map[string]WipeMDSFunc

// RegisterMetadataWipe is the hook used for implementations of
// MetadataServices to register their ways of deleting their metadata from the consistent store
func RegisterMetadataWipe(name string, newFunc WipeMDSFunc) {
	if wipeMDSFuncs == nil {
		wipeMDSFuncs = make(map[string]WipeMDSFunc)
	}

	if _, ok := wipeMDSFuncs[name]; ok {
		panic("torus: attempted to register WipeMDSFunc " + name + " twice")
	}

	wipeMDSFuncs[name] = newFunc
}

func WipeMDS(name string, cfg Config) error {
	clog.Debugf("running WipeMDS for service type: %s", name)
	return wipeMDSFuncs[name](cfg)
}

type SetRingFunc func(cfg Config, r Ring) error

var setRingFuncs map[string]SetRingFunc

// RegisterSetRing is the hook used for implementations of MetadataServices to
// register their ways of creating base metadata to the system.
func RegisterSetRing(name string, newFunc SetRingFunc) {
	if setRingFuncs == nil {
		setRingFuncs = make(map[string]SetRingFunc)
	}

	if _, ok := setRingFuncs[name]; ok {
		panic("torus: attempted to register SetRingFunc " + name + " twice")
	}

	setRingFuncs[name] = newFunc
}

// SetRing calls the specific SetRing function provided by a metadata package.
func SetRing(name string, cfg Config, r Ring) error {
	clog.Debugf("running setRing for service type: %s", name)
	return setRingFuncs[name](cfg, r)
}
