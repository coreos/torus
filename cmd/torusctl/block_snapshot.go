package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/coreos/torus"
	"github.com/coreos/torus/block"
	"github.com/spf13/cobra"
)

var (
	blockSnapshotCommand = &cobra.Command{
		Use:   "snapshot",
		Short: "manipulate snapshots for a block volume",
		Run:   blockAction,
	}

	bsnapListCommand = &cobra.Command{
		Use:   "list VOLUME",
		Short: "list snapshots for a block volume",
		Run: func(cmd *cobra.Command, args []string) {
			err := bsnapListAction(cmd, args)
			if err == torus.ErrUsage {
				cmd.Usage()
				os.Exit(1)
			} else if err != nil {
				die("%v", err)
			}
		},
	}

	bsnapCreateCommand = &cobra.Command{
		Use:   "create VOLUME@SNAPSHOT_NAME",
		Short: "create a snapshot for a block volume",
		Run: func(cmd *cobra.Command, args []string) {
			err := bsnapCreateAction(cmd, args)
			if err == torus.ErrUsage {
				cmd.Usage()
				os.Exit(1)
			} else if err != nil {
				die("%v", err)
			}
		},
	}

	bsnapDeleteCommand = &cobra.Command{
		Use:   "delete VOLUME@SNAPSHOT_NAME",
		Short: "delete a snapshot for a block volume",
		Run: func(cmd *cobra.Command, args []string) {
			err := bsnapDeleteAction(cmd, args)
			if err == torus.ErrUsage {
				cmd.Usage()
				os.Exit(1)
			} else if err != nil {
				die("%v", err)
			}
		},
	}

	bsnapRestoreCommand = &cobra.Command{
		Use:   "restore VOLUME@SNAPSHOT_NAME",
		Short: "restore VOLUME to the state it had as of SNAPSHOT_NAME",
		Run: func(cmd *cobra.Command, args []string) {
			err := bsnapRestoreAction(cmd, args)
			if err == torus.ErrUsage {
				cmd.Usage()
				os.Exit(1)
			} else if err != nil {
				die("%v", err)
			}
		},
	}
)

type SnapName struct {
	Volume   string
	Snapshot string
}

func ParseSnapName(s string) SnapName {
	p := strings.SplitN(s, "@", 2)
	if len(p) != 2 {
		return SnapName{s, ""}
	}
	return SnapName{p[0], p[1]}
}

func init() {
	blockCommand.AddCommand(blockSnapshotCommand)
	blockSnapshotCommand.AddCommand(bsnapListCommand)
	blockSnapshotCommand.AddCommand(bsnapCreateCommand)
	blockSnapshotCommand.AddCommand(bsnapDeleteCommand)
	blockSnapshotCommand.AddCommand(bsnapRestoreCommand)
	bsnapListCommand.Flags().BoolVarP(&outputAsCSV, "csv", "", false, "output as csv instead")
}

func bsnapListAction(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return torus.ErrUsage
	}
	vol := args[0]
	srv := createServer()
	defer srv.Close()
	blockvol, err := block.OpenBlockVolume(srv, vol)
	if err != nil {
		return fmt.Errorf("couldn't open block volume %s: %v", vol, err)
	}
	snaps, err := blockvol.GetSnapshots()
	if err != nil {
		return fmt.Errorf("couldn't get snapshots for block volume %s: %v", vol, err)
	}
	table := NewTableWriter(os.Stdout)
	table.SetHeader([]string{"Snapshot Name", "Timestamp"})
	for _, x := range snaps {
		table.Append([]string{
			x.Name,
			x.When.Format(time.RFC3339),
		})
	}
	if !outputAsCSV {
		fmt.Printf("Volume: %s\n", vol)
		table.Render()
	} else {
		table.RenderCSV()
	}
	return nil
}

func bsnapCreateAction(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return torus.ErrUsage
	}
	vol := ParseSnapName(args[0])
	if vol.Snapshot == "" {
		return fmt.Errorf("can't create snapshot without a name, please use the form VOLUME@SNAPSHOT_NAME")
	}
	srv := createServer()
	defer srv.Close()
	blockvol, err := block.OpenBlockVolume(srv, vol.Volume)
	if err != nil {
		return fmt.Errorf("couldn't open block volume %s: %v", vol.Volume, err)
	}
	err = blockvol.SaveSnapshot(vol.Snapshot)
	if err != nil {
		return fmt.Errorf("couldn't snapshot: %v", err)
	}
	return nil
}

func bsnapDeleteAction(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return torus.ErrUsage
	}
	vol := ParseSnapName(args[0])
	if vol.Snapshot == "" {
		return fmt.Errorf("can't delete a snapshot without a name, please use the form VOLUME@SNAPSHOT_NAME")
	}
	srv := createServer()
	defer srv.Close()
	blockvol, err := block.OpenBlockVolume(srv, vol.Volume)
	if err != nil {
		return fmt.Errorf("couldn't open block volume %s: %v", vol.Volume, err)
	}
	err = blockvol.DeleteSnapshot(vol.Snapshot)
	if err != nil {
		return fmt.Errorf("couldn't delete snapshot: %v", err)
	}
	return nil
}

func bsnapRestoreAction(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return torus.ErrUsage
	}
	vol := ParseSnapName(args[0])
	if vol.Snapshot == "" {
		return fmt.Errorf("can't restore a snapshot without a name, please use the form VOLUME@SNAPSHOT_NAME")
	}
	srv := createServer()
	defer srv.Close()
	blockvol, err := block.OpenBlockVolume(srv, vol.Volume)
	if err != nil {
		return fmt.Errorf("couldn't open block volume %s: %v", vol.Volume, err)
	}
	err = blockvol.RestoreSnapshot(vol.Snapshot)
	if err != nil {
		return fmt.Errorf("couldn't restore snapshot: %v", err)
	}
	return nil
}
