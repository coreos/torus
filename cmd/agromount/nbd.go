package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/coreos/agro"
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
	err := connectNBD(srv, args[0], knownDev, closer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
	srv.Close()
}

func connectNBD(srv agro.Server, vol string, target string, closer chan bool) error {
	blocksrv, err := srv.Block()
	if err != nil {
		fmt.Fprintf(os.Stderr, "server doesn't support block volumes: %s\n", err)
		return err
	}

	f, err := blocksrv.OpenBlockFile(vol)
	if err != nil {
		fmt.Fprintf(os.Stderr, "can't open block volume: %s\n", err)
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	stats, err := srv.Info()
	if err != nil {
		return err
	}

	handle := nbd.Create(f, int64(fi.Size()), int64(stats.BlockSize))

	var dev string
	if target != "" {
		dev, err = handle.ConnectDevice(target)
	} else {
		dev, err = handle.Connect()
	}
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
