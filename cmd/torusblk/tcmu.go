// +build linux

package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/coreos/torus"
	"github.com/coreos/torus/block"
	"github.com/spf13/cobra"

	"github.com/coreos/torus/internal/tcmu"
)

var (
	tcmuCommand = &cobra.Command{
		Use:   "tcmu VOLUME",
		Short: "attach a torus block volume via SCSI",
		Run: func(cmd *cobra.Command, args []string) {
			err := tcmuAction(cmd, args)
			if err == torus.ErrUsage {
				cmd.Usage()
				os.Exit(1)
			} else if err != nil {
				die("%v", err)
			}
		},
	}
)

func init() {
	rootCommand.AddCommand(tcmuCommand)
}

func tcmuAction(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return torus.ErrUsage
	}

	srv := createServer()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	closer := make(chan bool)
	go func() {
		for range signalChan {
			fmt.Println("\nReceived an interrupt, disconnecting...")
			close(closer)
		}
	}()
	defer srv.Close()
	blockvol, err := block.OpenBlockVolume(srv, args[0])
	if err != nil {
		return fmt.Errorf("server doesn't support block volumes: %s", err)
	}

	f, err := blockvol.OpenBlockFile()
	if err != nil {
		if err == torus.ErrLocked {
			return fmt.Errorf("volume %s is already mounted on another host", args[0])
		}
		return fmt.Errorf("can't open block volume: %s", err)
	}
	defer f.Close()
	err = torustcmu.ConnectAndServe(f, args[0], closer)
	if err != nil {
		return fmt.Errorf("failed to serve volume using SCSI: %s", err)
	}
	return nil
}
