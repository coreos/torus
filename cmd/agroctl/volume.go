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
	if len(args) == 0 {
		cmd.Usage()
		os.Exit(1)
	}
	if len(args) != 1 {
		cmd.Usage()
		die("too many volumes specified (try one at a time)")
	}
	mds := mustConnectToMDS()
	err := mds.CreateVolume(&models.Volume{
		Name: args[0],
		Type: models.Volume_FILE,
	})
	if err != nil {
		die("error creating volume %s: %v", args[0], err)
	}
}

func volumeCreateBlockAction(cmd *cobra.Command, args []string) {
	mds := mustConnectToMDS()
	if len(args) != 2 {
		cmd.Usage()
		os.Exit(1)
	}
	size, err := humanize.ParseBytes(args[1])
	if err != nil {
		die("error parsing size %s: %v", args[1], err)
	}
	err = mds.CreateVolume(&models.Volume{
		Name:     args[0],
		MaxBytes: size,
		Type:     models.Volume_BLOCK,
	})
	if err != nil {
		die("error creating volume %s: %v", args[0], err)
	}
}

func volumeListAction(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Usage()
		os.Exit(1)
	}
	mds := mustConnectToMDS()
	vols, err := mds.GetVolumes()
	if err != nil {
		die("error listing volumes: %v\n", err)
	}
	for _, x := range vols {
		fmt.Println(x)
	}
}
