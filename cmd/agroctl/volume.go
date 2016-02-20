package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var volumeCommand = &cobra.Command{
	Use:   "volume",
	Short: "manage volumes in the cluster",
	Run:   volumeAction,
}

var volumeCreateCommand = &cobra.Command{
	Use:   "create",
	Short: "create a volume in the cluster",
	Run:   volumeCreateAction,
}

var volumeListCommand = &cobra.Command{
	Use:   "list",
	Short: "list volumes in the cluster",
	Run:   volumeListAction,
}

func init() {
	volumeCommand.AddCommand(volumeCreateCommand)
	volumeCommand.AddCommand(volumeListCommand)
}

func volumeAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
}

func volumeCreateAction(cmd *cobra.Command, args []string) {
	mds := mustConnectToMDS()
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "USAGE: agroctl volume create VOLUME_NAME")
		os.Exit(1)
	}
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "too many volumes specified (try one at a time)\n")
		os.Exit(1)
	}
	err := mds.CreateVolume(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating volume %s: %s", args[0], err)
		os.Exit(1)
	}
}

func volumeListAction(cmd *cobra.Command, args []string) {
	mds := mustConnectToMDS()
	if len(args) != 0 {
		fmt.Fprintf(os.Stderr, "too many arguments to list\n")
		os.Exit(1)
	}
	vols, err := mds.GetVolumes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error listing volumes: %s", err)
		os.Exit(1)
	}
	for _, x := range vols {
		fmt.Println(x)
	}
}
