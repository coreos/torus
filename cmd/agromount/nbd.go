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

var (
	nbdCommand = &cobra.Command{
		Use:   "nbd VOLUME [NBD-DEV]",
		Short: "attach a block volume to an NBD device",
		Run:   nbdAction,
	}

	nbdServeCommand = &cobra.Command{
		Use:   "nbdserve",
		Short: "serve a block volume over the NBD protocol",
		Run:   nbdServeAction,
	}

	serveListenAddress string
)

func init() {
	rootCommand.AddCommand(nbdCommand)

	nbdServeCommand.Flags().StringVarP(&serveListenAddress, "listen", "l", "0.0.0.0:10809", "nbd server listen address")
	rootCommand.AddCommand(nbdServeCommand)
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
	defer f.Close()
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

type finder struct {
	srv *agro.Server
}

func (f *finder) FindDevice(name string) (nbd.Device, error) {
	blockvol, err := block.OpenBlockVolume(f.srv, name)
	if err != nil {
		return nil, err
	}

	dev, err := blockvol.OpenBlockFile()
	if err != nil {
		return nil, err
	}

	return dev, nil
}

func (f *finder) ListDevices() ([]string, error) {
	vols, _, err := f.srv.MDS.GetVolumes()
	if err != nil {
		return nil, err
	}

	var volnames []string

	for _, v := range vols {
		volnames = append(volnames, v.Name)
	}

	return volnames, nil
}

func nbdServeAction(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Usage()
		os.Exit(1)
	}

	srv := createServer()
	defer srv.Close()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	devfinder := &finder{srv}
	server, err := nbd.NewNBDServer(serveListenAddress, devfinder)
	if err != nil {
		die("can't start server: %v", err)
	}

	// TODO: sync all conns
	go func() {
		<-signalChan
		server.Close()
		return
	}()

	if err := server.Serve(); err != nil {
		die("server exited: %v", err)
	}
}
