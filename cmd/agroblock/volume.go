package main

import (
	"os"

	"github.com/coreos/agro"
	"github.com/coreos/agro/block"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

var volumeCommand = &cobra.Command{
	Use:   "volume",
	Short: "manage volumes in the cluster",
	Run:   volumeAction,
}

var volumeCreateCommand = &cobra.Command{
	Use:   "create NAME SIZE",
	Short: "create a block volume in the cluster",
	Long:  "creates a block volume named NAME of size SIZE bytes (G,GiB,M,MiB,etc suffixes accepted)",
	Run:   volumeCreateAction,
}

func init() {
	volumeCommand.AddCommand(volumeCreateCommand)
}

func volumeAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
}

func volumeCreateAction(cmd *cobra.Command, args []string) {
	mds := mustConnectToMDS()
	if len(args) != 2 {
		cmd.Usage()
		os.Exit(1)
	}
	size, err := humanize.ParseBytes(args[1])
	if err != nil {
		die("error parsing size %s: %v", args[1], err)
	}
	err = block.CreateBlockVolume(mds, args[0], size)
	if err != nil {
		die("error creating volume %s: %v", args[0], err)
	}
}

func mustConnectToMDS() agro.MetadataService {
	cfg := agro.Config{
		MetadataAddress: etcdAddress,
	}
	mds, err := agro.CreateMetadataService("etcd", cfg)
	if err != nil {
		die("couldn't connect to etcd: %v", err)
	}
	return mds
}
