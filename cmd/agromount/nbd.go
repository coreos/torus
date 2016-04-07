package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/coreos/agro"
	"github.com/coreos/agro/block"
	"github.com/coreos/agro/internal/nbd"

	"github.com/spf13/cobra"
)

var nbdCommand = &cobra.Command{
	Use:   "nbd VOLUME [NBD-DEV]",
	Short: "attach a block volume to an NBD device",
	Run:   nbdAction,
}

func nbdAction(cmd *cobra.Command, args []string) {

	if len(args) != 1 && len(args) != 2 {
		cmd.Usage()
		os.Exit(1)
	}

	var knownDev string
	if len(args) == 2 {
		knownDev = args[1]
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
		fmt.Fprintf(os.Stderr, "can't open block volume: %s\n", err)
		os.Exit(1)
	}
	err = connectNBD(srv, f, knownDev, closer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func connectNBD(srv *agro.Server, f *block.BlockFile, target string, closer chan bool) error {
	defer f.Close()
	size := f.Size()

	gmd, err := srv.MDS.GlobalMetadata()
	if err != nil {
		return err
	}

	handle := nbd.Create(f, int64(size), int64(gmd.BlockSize))

	if target == "" {
		target, err = nbd.FindDevice()
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	dev, err := handle.ConnectDevice(target)
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Println("Connected to", dev)

	go func(n *nbd.NBD) {
		<-closer
		n.Close()
	}(handle)

	return handle.Serve()
}
