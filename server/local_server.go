package server

import (
	"os"
	"path/filepath"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/server/gc"
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
	err = os.MkdirAll(filepath.Join(dir, "inode"), 0700)
	if err != nil {
		return err
	}
	return nil
}

func NewServer(cfg agro.Config, metadataServiceName, inodeStoreName, blockStoreName string) (agro.Server, error) {
	err := mkdirsFor(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	mds, err := agro.CreateMetadataService(metadataServiceName, cfg)
	if err != nil {
		return nil, err
	}

	global, err := mds.GlobalMetadata()
	if err != nil {
		return nil, err
	}

	inodes, err := agro.CreateINodeStore(inodeStoreName, cfg)
	if err != nil {
		return nil, err
	}

	blocks, err := agro.CreateBlockStore(blockStoreName, cfg, global)
	if err != nil {
		return nil, err
	}

	s := &server{
		blocks:        blocks,
		mds:           mds,
		inodes:        inodes,
		peersMap:      make(map[string]*models.PeerInfo),
		openINodeRefs: make(map[string]map[agro.INodeID]int),
		gc:            gc.NewGCController(mds, blocks),
	}
	s.gc.Start()
	return s, nil
}

func NewMemoryServer() agro.Server {
	cfg := agro.Config{}
	x, err := NewServer(cfg, "memory", "temp", "temp")
	if err != nil {
		panic(err)
	}
	return x
}
