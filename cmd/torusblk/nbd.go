package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/coreos/torus"
	"github.com/coreos/torus/block"
	"github.com/coreos/torus/internal/nbd"

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
)

var (
	serveListenAddress string
	detachDevice       string
)

func init() {
	rootCommand.AddCommand(nbdCommand)
	rootCommand.AddCommand(nbdServeCommand)

	nbdCommand.Flags().StringVarP(&detachDevice, "detach", "d", "", "detach an NBD device from a block volume. (e.g. torsublk nbd -d /dev/nbd0)")
	nbdServeCommand.Flags().StringVarP(&serveListenAddress, "listen", "l", "0.0.0.0:10809", "nbd server listen address")
}

func nbdAction(cmd *cobra.Command, args []string) {
	if len(detachDevice) > 0 && len(args) == 0 {
		if err := nbd.Detach(detachDevice); err != nil {
			die("failed to detach: %v", err)
		}
		os.Exit(0)
	}

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
		if err == torus.ErrLocked {
			fmt.Fprintf(os.Stderr, "volume %s is already mounted on another host\n", args[0])
		} else {
			fmt.Fprintf(os.Stderr, "can't open block volume: %s\n", err)
		}
		os.Exit(1)
	}
	defer f.Close()
	err = connectNBD(srv, f, knownDev, closer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func connectNBD(srv *torus.Server, f *block.BlockFile, target string, closer chan bool) error {
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
			return err
		}
	}

	dev, err := handle.OpenDevice(target)
	if err != nil {
		return err
	}
	fmt.Println("Connected to", dev)

	go func(n *nbd.NBD) {
		<-closer
		n.Disconnect()
	}(handle)

	err = handle.Serve()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error from nbd server: %s\n", err)
		os.Exit(1)
	}
	return nil
}

type finder struct {
	srv *torus.Server
}

func (f *finder) FindDevice(name string) (nbd.Device, error) {
	blockvol, err := block.OpenBlockVolume(f.srv, name)
	if err != nil {
		return nil, err
	}

	return blockvol.OpenBlockFile()
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
