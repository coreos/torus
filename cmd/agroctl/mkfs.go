package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/blockset"
	"github.com/spf13/cobra"

	_ "github.com/barakmich/agro/metadata/etcd"
)

var (
	blockSize int
	blockSpec string
)

var mkfsCommand = &cobra.Command{
	Use:    "mkfs",
	Short:  "Prepare a new filesystem by creating the metadata",
	PreRun: mkfsPreRun,
	Run:    mkfsAction,
}

func init() {
	mkfsCommand.Flags().IntVarP(&blockSize, "block-size", "", 8196, "size of all data blocks in this filesystem")
	mkfsCommand.Flags().StringVarP(&blockSpec, "block-spec", "", "crc", "default replication/error correction applied to blocks in this filesystem")
}

func mkfsPreRun(cmd *cobra.Command, args []string) {
	// We *always* need base.
	if !strings.HasSuffix(blockSpec, ",base") {
		blockSpec += ",base"
	}
}

func mkfsAction(cmd *cobra.Command, args []string) {
	var err error
	md := agro.GlobalMetadata{}
	md.BlockSize = uint64(blockSize)
	md.DefaultBlockSpec, err = blockset.ParseBlockLayerSpec(blockSpec)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing block-spec: %s\n", err)
		os.Exit(1)
	}

	cfg := agro.Config{
		MetadataAddress: etcdAddress,
	}
	err = agro.Mkfs("etcd", cfg, md)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error writing metadata: %s\n", err)
		os.Exit(1)
	}
}
