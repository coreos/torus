package main

import (
	"os"

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

func init() {
	blockCommand.AddCommand(blockCreateCommand)
	flagconfig.AddConfigFlags(blockCommand.PersistentFlags())
}

func blockAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
}
