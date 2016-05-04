package main

import (
	"fmt"
	"os"

	"github.com/coreos/agro/block"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

var volumeCommand = &cobra.Command{
	Use:   "volume",
	Short: "manage volumes in the cluster",
	Run:   volumeAction,
}

var volumeCreateBlockCommand = &cobra.Command{
	Use:   "create-block",
	Short: "create a block volume in the cluster",
	Run:   volumeCreateBlockAction,
}
var volumeDeleteCommand = &cobra.Command{
	Use:   "delete",
	Short: "delete a volume in the cluster",
	Run:   volumeDeleteAction,
}

var volumeListCommand = &cobra.Command{
	Use:   "list",
	Short: "list volumes in the cluster",
	Run:   volumeListAction,
}

func init() {
	volumeCommand.AddCommand(volumeCreateBlockCommand)
	volumeCommand.AddCommand(volumeDeleteCommand)
	volumeCommand.AddCommand(volumeListCommand)
}

func volumeAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
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
	err = block.CreateBlockVolume(mds, args[0], size)
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

func volumeDeleteAction(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}
	name := args[0]
	mds := mustConnectToMDS()
	vol, err := mds.GetVolume(name)
	if err != nil {
		die("cannot get volume %s (perhaps it doesn't exist): %v", name, err)
	}
	switch vol.Type {
	case "block":
		err = block.DeleteBlockVolume(mds, name)
	default:
		die("unknown volume type %s", vol.Type)
	}
	if err != nil {
		die("cannot delete volume: %v", err)
	}
}
