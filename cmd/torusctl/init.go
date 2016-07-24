package main

import (
	"strings"

	"github.com/coreos/torus"
	"github.com/coreos/torus/blockset"
	"github.com/coreos/torus/internal/flagconfig"
	"github.com/coreos/torus/ring"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"

	_ "github.com/coreos/torus/metadata/etcd"
)

var (
	blockSize        uint64
	blockSizeStr     string
	blockSpec        string
	inodeReplication int
	noMakeRing       bool
)

var initCommand = &cobra.Command{
	Use:    "init",
	Short:  "Prepare a new torus cluster by creating the metadata",
	PreRun: initPreRun,
	Run:    initAction,
}

func init() {
	initCommand.Flags().StringVarP(&blockSizeStr, "block-size", "", "512KiB", "size of all data blocks in this storage cluster")
	initCommand.Flags().StringVarP(&blockSpec, "block-spec", "", "crc", "default replication/error correction applied to blocks in this storage cluster")
	initCommand.Flags().IntVarP(&inodeReplication, "inode-replication", "", 3, "default number of times to replicate inodes across the cluster")
	initCommand.Flags().BoolVar(&noMakeRing, "no-ring", false, "do not create the default ring as part of init")
}

func initPreRun(cmd *cobra.Command, args []string) {
	// We *always* need base.
	if !strings.HasSuffix(blockSpec, ",base") {
		blockSpec += ",base"
	}
	var err error
	blockSize, err = humanize.ParseBytes(blockSizeStr)
	if err != nil {
		die("error parsing block-size: %v", err)
	}
}

func initAction(cmd *cobra.Command, args []string) {
	var err error
	md := torus.GlobalMetadata{}
	md.BlockSize = blockSize
	md.DefaultBlockSpec, err = blockset.ParseBlockLayerSpec(blockSpec)
	md.INodeReplication = inodeReplication
	if err != nil {
		die("error parsing block-spec: %v", err)
	}

	cfg := flagconfig.BuildConfigFromFlags()
	ringType := ring.Ketama
	if noMakeRing {
		ringType = ring.Empty
	}
	err = torus.InitMDS("etcd", cfg, md, ringType)
	if err != nil {
		die("error writing metadata: %v", err)
	}
}
