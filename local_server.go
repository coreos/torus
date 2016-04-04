package agro

import (
	"os"
	"path/filepath"

	"github.com/coreos/agro/models"
)

func mkdirsFor(dir string) error {
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
	err := mkdirsFor(cfg.DataDir)
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

	s := &Server{
		blocks:   blocks,
		MDS:      mds,
		inodes:   NewINodeStore(blocks),
		peersMap: make(map[string]*models.PeerInfo),
		cfg:      cfg,
		peerInfo: &models.PeerInfo{
			UUID: mds.UUID(),
		},
	}
	return s, nil
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
