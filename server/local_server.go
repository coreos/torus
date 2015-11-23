package server

import (
	"os"
	"path/filepath"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/storage/block"
	"github.com/barakmich/agro/storage/inode"

	_ "github.com/barakmich/agro/metadata/temp"
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

func NewPersistentServer(cfg agro.Config) (agro.Server, error) {
	err := mkdirsFor(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	mds, err := agro.CreateMetadataService("temp", cfg)
	if err != nil {
		return nil, err
	}
	global, err := mds.GlobalMetadata()
	if err != nil {
		return nil, err
	}
	inodes, err := inode.OpenBoltInodeStore(cfg)
	if err != nil {
		return nil, err
	}
	blocks, err := block.NewMFileBlockStorage(cfg, global)
	if err != nil {
		return nil, err
	}
	return &server{
		cold:   blocks,
		mds:    mds,
		inodes: inodes,
	}, nil
}
