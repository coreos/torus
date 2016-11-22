package main

import (
	"os"

	"github.com/coreos/torus"
	"github.com/coreos/torus/internal/flagconfig"
	"github.com/spf13/cobra"
)

var blockCommand = &cobra.Command{
	Use:   "block",
	Short: "interact with block volumes",
	Run:   blockAction,
}

var blockCreateCommand = &cobra.Command{
	Use:   "create NAME SIZE",
	Short: "create a block volume in the cluster",
	Long:  "creates a block volume named NAME of size SIZE bytes (G,GiB,M,MiB,etc suffixes accepted)",
	Run:   volumeCreateBlockAction,
}

var blockCreateFromSnapshotCommand = &cobra.Command{
	Use:   "from-snapshot VOLUME@SNAPSHOT_NAME NEW_NAME",
	Short: "create a block volume from snapshot",
	Long:  "creates a block volume named NAME from snapshot",
	Run: func(cmd *cobra.Command, args []string) {
		err := volumeCreateBlockFromSnapshotAction(cmd, args)
		if err == torus.ErrUsage {
			cmd.Usage()
			os.Exit(1)
		} else if err != nil {
			die("%v", err)
		}
	},
}

func init() {
	blockCommand.AddCommand(blockCreateCommand)
	blockCreateCommand.AddCommand(blockCreateFromSnapshotCommand)
	flagconfig.AddConfigFlags(blockCommand.PersistentFlags())
}

func blockAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
}
