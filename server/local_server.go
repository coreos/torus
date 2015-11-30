package server

import (
	"os"
	"path/filepath"

	"github.com/barakmich/agro"
)

func mkdirsFor(dir string) error {
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

	return &server{
		blocks: blocks,
		mds:    mds,
		inodes: inodes,
	}, nil
}
