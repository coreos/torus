package main

import (
	"fmt"
	"os"

	"github.com/coreos/agro/models"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

var volumeCommand = &cobra.Command{
	Use:   "volume",
	Short: "manage volumes in the cluster",
	Run:   volumeAction,
}

var volumeCreateFSCommand = &cobra.Command{
	Use:   "create-fs",
	Short: "create a volume in the cluster",
	Run:   volumeCreateFSAction,
}

var volumeCreateBlockCommand = &cobra.Command{
	Use:   "create-block",
	Short: "create a block volume in the cluster",
	Run:   volumeCreateBlockAction,
}

var volumeListCommand = &cobra.Command{
	Use:   "list",
	Short: "list volumes in the cluster",
	Run:   volumeListAction,
}

func init() {
	volumeCommand.AddCommand(volumeCreateFSCommand)
	volumeCommand.AddCommand(volumeCreateBlockCommand)
	volumeCommand.AddCommand(volumeListCommand)
}

func volumeAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
}

func volumeCreateFSAction(cmd *cobra.Command, args []string) {
	mds := mustConnectToMDS()
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "USAGE: agroctl volume create-fs VOLUME_NAME")
		os.Exit(1)
	}
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "too many volumes specified (try one at a time)\n")
		os.Exit(1)
	}
	err := mds.CreateVolume(&models.Volume{
		Name: args[0],
		Type: models.Volume_FILE,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating volume %s: %s", args[0], err)
		os.Exit(1)
	}
}

func volumeCreateBlockAction(cmd *cobra.Command, args []string) {
	mds := mustConnectToMDS()
	if len(args) != 2 {
		fmt.Fprintf(os.Stderr, "USAGE: agroctl volume create-block VOLUME_NAME SIZE")
		os.Exit(1)
	}
	size, err := humanize.ParseBytes(args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing size %s", args[1])
	}
	err = mds.CreateVolume(&models.Volume{
		Name:     args[0],
		MaxBytes: size,
		Type:     models.Volume_BLOCK,
	})
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
