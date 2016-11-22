package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/coreos/pkg/progressutil"
	"github.com/coreos/torus"
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

var volumeCreateBlockFromSnapshotCommand = &cobra.Command{
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
	volumeCommand.AddCommand(volumeDeleteCommand)
	volumeCommand.AddCommand(volumeListCommand)
	volumeCommand.AddCommand(volumeCreateBlockCommand)
	volumeCreateBlockCommand.AddCommand(volumeCreateBlockFromSnapshotCommand)
	volumeCreateBlockFromSnapshotCommand.Flags().BoolVarP(&progress, "progress", "p", false, "show progress")
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

func volumeCreateBlockFromSnapshotAction(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return torus.ErrUsage
	}
	snapshot := args[0]
	newVolName := args[1]

	vol := ParseSnapName(snapshot)
	if vol.Snapshot == "" {
		return fmt.Errorf("can't restore a snapshot without a name, please use the form VOLUME@SNAPSHOT_NAME")
	}

	srv := createServer()
	defer srv.Close()

	// open original snapshot
	blockvolSrc, err := block.OpenBlockVolume(srv, vol.Volume)
	if err != nil {
		return fmt.Errorf("couldn't open block volume %s: %v", vol.Volume, err)
	}
	bfsrc, err := blockvolSrc.OpenSnapshot(vol.Snapshot)
	if err != nil {
		return fmt.Errorf("couldn't open snapshot: %v", err)
	}
	size := bfsrc.Size()

	// create  new volume
	err = block.CreateBlockVolume(srv.MDS, newVolName, size)
	if err != nil {
		return fmt.Errorf("error creating volume %s: %v", newVolName, err)
	}
	blockvolDist, err := block.OpenBlockVolume(srv, newVolName)
	if err != nil {
		return fmt.Errorf("couldn't open block volume %s: %v", newVolName, err)
	}
	bfdist, err := blockvolDist.OpenBlockFile()
	if err != nil {
		return fmt.Errorf("couldn't open blockfile %s: %v", newVolName, err)
	}
	defer bfdist.Close()

	if progress {
		pb := progressutil.NewCopyProgressPrinter()
		pb.AddCopy(bfsrc, newVolName, int64(size), bfdist)
		err := pb.PrintAndWait(os.Stderr, 500*time.Millisecond, nil)
		if err != nil {
			return fmt.Errorf("couldn't copy: %v", err)
		}
	} else {
		n, err := io.Copy(bfdist, bfsrc)
		if err != nil {
			return fmt.Errorf("couldn't copy: %v", err)
		}
		if n != int64(size) {
			return fmt.Errorf("copied size %d doesn't match original size %d", n, size)
		}
	}
	err = bfdist.Sync()
	if err != nil {
		return fmt.Errorf("couldn't sync: %v", err)
	}
	return nil
}
