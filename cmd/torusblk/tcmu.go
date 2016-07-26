// +build linux

package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/coreos/torus"
	"github.com/coreos/torus/block"
	"github.com/coreos/torus/internal/tcmu"
	"github.com/spf13/cobra"
)

var (
	tcmuCommand = &cobra.Command{
		Use:   "tcmu VOLUME",
		Short: "attach a torus block volume via SCSI",
		Run:   tcmuAction,
	}
)

func init() {
	rootCommand.AddCommand(tcmuCommand)
}

func tcmuAction(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}

	srv := createServer()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	closer := make(chan bool)
	go func() {
		for _ = range signalChan {
			fmt.Println("\nReceived an interrupt, disconnecting...")
			close(closer)
		}
	}()
	defer srv.Close()
	blockvol, err := block.OpenBlockVolume(srv, args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "server doesn't support block volumes: %s\n", err)
		os.Exit(1)
	}

	f, err := blockvol.OpenBlockFile()
	if err != nil {
		if err == torus.ErrLocked {
			fmt.Fprintf(os.Stderr, "volume %s is already mounted on another host\n", args[0])
		} else {
			fmt.Fprintf(os.Stderr, "can't open block volume: %s\n", err)
		}
		os.Exit(1)
	}
	defer f.Close()
	err = torustcmu.ConnectAndServe(f, args[0], closer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
	}
}
