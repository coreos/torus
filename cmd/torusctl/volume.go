package main

import (
	"os"

	"github.com/coreos/torus/block"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

var volumeCommand = &cobra.Command{
	Use:   "volume",
	Short: "manage volumes in the cluster",
	Run:   volumeAction,
}

var volumeDeleteCommand = &cobra.Command{
	Use:   "delete NAME",
	Short: "delete a volume in the cluster",
	Run:   volumeDeleteAction,
}

var volumeListCommand = &cobra.Command{
	Use:   "list",
	Short: "list volumes in the cluster",
	Run:   volumeListAction,
}

var volumeCreateBlockCommand = &cobra.Command{
	Use:   "create-block NAME SIZE",
	Short: "create a block volume in the cluster",
	Long:  "creates a block volume named NAME of size SIZE bytes (G,GiB,M,MiB,etc suffixes accepted)",
	Run:   volumeCreateBlockAction,
}

func init() {
	volumeCommand.AddCommand(volumeDeleteCommand)
	volumeCommand.AddCommand(volumeListCommand)
	volumeCommand.AddCommand(volumeCreateBlockCommand)
	volumeListCommand.Flags().BoolVarP(&outputAsCSV, "csv", "", false, "output as csv instead")
	volumeListCommand.Flags().BoolVarP(&outputAsSI, "si", "", false, "output sizes in powers of 1000")
}

func volumeAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
}

func volumeListAction(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Usage()
		os.Exit(1)
	}
	mds := mustConnectToMDS()
	vols, _, err := mds.GetVolumes()
	if err != nil {
		die("error listing volumes: %v\n", err)
	}
	table := NewTableWriter(os.Stdout)
	table.SetHeader([]string{"Volume Name", "Size", "Type"})
	for _, x := range vols {
		table.Append([]string{
			x.Name,
			bytesOrIbytes(x.MaxBytes, outputAsSI),
			x.Type,
		})
	}
	if outputAsCSV {
		table.RenderCSV()
		return
	}
	table.Render()
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
