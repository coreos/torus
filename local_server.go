package torus

import (
	"os"
	"path/filepath"

	"github.com/coreos/torus/models"
)

func MkdirsFor(dir string) error {
	if dir == "" {
		return nil
	}
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		return err
	}
	err = os.MkdirAll(filepath.Join(dir, "metadata"), 0700)
	if err != nil {
		return err
	}
	err = os.MkdirAll(filepath.Join(dir, "block"), 0700)
	if err != nil {
		return err
	}
	return nil
}

func NewServer(cfg Config, metadataServiceKind, blockStoreKind string) (*Server, error) {
	err := MkdirsFor(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	mds, err := CreateMetadataService(metadataServiceKind, cfg)
	if err != nil {
		return nil, err
	}

	global, err := mds.GlobalMetadata()
	if err != nil {
		return nil, err
	}

	blocks, err := CreateBlockStore(blockStoreKind, "current", cfg, global)
	if err != nil {
		return nil, err
	}
	return NewServerByImpl(cfg, mds, blocks)
}

func NewMemoryServer() *Server {
	cfg := Config{
		StorageSize: 100 * 1024 * 1024,
	}
	x, err := NewServer(cfg, "temp", "temp")
	if err != nil {
		panic(err)
	}
	return x
}

func NewServerByImpl(cfg Config, mds MetadataService, blocks BlockStore) (*Server, error) {
	return &Server{
		Blocks:   blocks,
		MDS:      mds,
		INodes:   NewINodeStore(blocks),
		peersMap: make(map[string]*models.PeerInfo),
		Cfg:      cfg,
		peerInfo: &models.PeerInfo{
			UUID: mds.UUID(),
		},
	}, nil
}
