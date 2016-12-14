package main

import (
	"fmt"
	"os"
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
	metaView         bool
	initRingType     string
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
	initCommand.Flags().BoolVar(&metaView, "view", false, "view metadata configured in this storage cluster")
	initCommand.Flags().StringVar(&initRingType, "type", "ketama", "type of ring to create (empty, single, mod or ketama)")
}

func initPreRun(cmd *cobra.Command, args []string) {
	if metaView {
		viewMetadata()
		os.Exit(0)
	}
	// We *always* need base.
	if !strings.HasSuffix(blockSpec, ",base") && !strings.HasPrefix(blockSpec, "base") {
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
	if err != nil {
		die("error parsing block-spec: %v", err)
	}

	cfg := flagconfig.BuildConfigFromFlags()

	var ringType torus.RingType
	switch initRingType {
	case "empty":
		ringType = ring.Empty
	case "single":
		ringType = ring.Single
	case "mod":
		ringType = ring.Mod
	case "ketama":
		ringType = ring.Ketama
	default:
		die(`invalid ring type %s (try "empty", "mod", "single" or "ketama")`, initRingType)
	}

	err = torus.InitMDS("etcd", cfg, md, ringType)
	if err != nil {
		die("error writing metadata: %v", err)
	}
}

func viewMetadata() {
	mds := mustConnectToMDS()
	md := mds.GlobalMetadata()

	var blockSpecToStrings = []string{
		blockset.Base:        "base",
		blockset.CRC:         "crc",
		blockset.Replication: "rep",
	}
	blockSpec := ""
	for _, x := range md.DefaultBlockSpec {
		blockSpec += blockSpecToStrings[x.Kind] + " "
	}
	fmt.Printf("Block size: %d byte\n", md.BlockSize)
	fmt.Printf("Block spec: %s\n", blockSpec)
}
