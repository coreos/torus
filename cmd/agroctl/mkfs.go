package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/blockset"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"

	_ "github.com/barakmich/agro/metadata/etcd"
)

var (
	blockSize    uint64
	blockSizeStr string
	blockSpec    string
)

var mkfsCommand = &cobra.Command{
	Use:    "mkfs",
	Short:  "Prepare a new filesystem by creating the metadata",
	PreRun: mkfsPreRun,
	Run:    mkfsAction,
}

func init() {
	mkfsCommand.Flags().StringVarP(&blockSizeStr, "block-size", "", "8KiB", "size of all data blocks in this filesystem")
	mkfsCommand.Flags().StringVarP(&blockSpec, "block-spec", "", "crc", "default replication/error correction applied to blocks in this filesystem")
}

func mkfsPreRun(cmd *cobra.Command, args []string) {
	// We *always* need base.
	if !strings.HasSuffix(blockSpec, ",base") {
		blockSpec += ",base"
	}
	var err error
	blockSize, err = humanize.ParseBytes(blockSizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing block-size: %s\n", err)
		os.Exit(1)
	}
}

func mkfsAction(cmd *cobra.Command, args []string) {
	var err error
	md := agro.GlobalMetadata{}
	md.BlockSize = blockSize
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
