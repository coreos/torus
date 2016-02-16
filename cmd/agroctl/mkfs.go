package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/ring"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"

	_ "github.com/coreos/agro/metadata/etcd"
)

var (
	blockSize        uint64
	blockSizeStr     string
	blockSpec        string
	inodeReplication int
	noMakeRing       bool
)

var mkfsCommand = &cobra.Command{
	Use:    "mkfs",
	Short:  "Prepare a new filesystem by creating the metadata",
	PreRun: mkfsPreRun,
	Run:    mkfsAction,
}

func init() {
	mkfsCommand.Flags().StringVarP(&blockSizeStr, "block-size", "", "512KiB", "size of all data blocks in this filesystem")
	mkfsCommand.Flags().StringVarP(&blockSpec, "block-spec", "", "crc", "default replication/error correction applied to blocks in this filesystem")
	mkfsCommand.Flags().IntVarP(&inodeReplication, "inode-replication", "", 3, "default number of times to replicate inodes across the cluster")
	mkfsCommand.Flags().BoolVar(&noMakeRing, "no-ring", false, "do not create the default ring as part of mkfs")
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
	md.INodeReplication = inodeReplication
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing block-spec: %s\n", err)
		os.Exit(1)
	}

	cfg := agro.Config{
		MetadataAddress: etcdAddress,
	}
	ringType := ring.Ketama
	if noMakeRing {
		ringType = ring.Empty
	}
	err = agro.Mkfs("etcd", cfg, md, ringType)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error writing metadata: %s\n", err)
		os.Exit(1)
	}
}
