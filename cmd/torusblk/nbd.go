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
		Run: func(cmd *cobra.Command, args []string) {
			err := nbdAction(cmd, args)
			if err == torus.ErrUsage {
				cmd.Usage()
				os.Exit(1)
			} else if err != nil {
				die("%v", err)
			}
		},
	}

	nbdServeCommand = &cobra.Command{
		Use:   "nbdserve",
		Short: "serve a block volume over the NBD protocol",
		Run: func(cmd *cobra.Command, args []string) {
			err := nbdServeAction(cmd, args)
			if err == torus.ErrUsage {
				cmd.Usage()
				os.Exit(1)
			} else if err != nil {
				die("%v", err)
			}
		},
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

func nbdAction(cmd *cobra.Command, args []string) error {
	if len(detachDevice) > 0 && len(args) == 0 {
		if err := nbd.Detach(detachDevice); err != nil {
			return fmt.Errorf("failed to detach: %v", err)
		}
		return nil
	}

	if len(args) != 1 && len(args) != 2 {
		return torus.ErrUsage
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
	err = connectNBD(srv, f, knownDev, closer)
	if err != nil {
		return err
	}
	return nil
}

func connectNBD(srv *torus.Server, f *block.BlockFile, target string, closer chan bool) error {
	defer f.Close()
	size := f.Size()

	gmd := srv.MDS.GlobalMetadata()

	handle := nbd.Create(f, int64(size), int64(gmd.BlockSize))

	if target == "" {
		t, err := nbd.FindDevice()
		if err != nil {
			return err
		}
		target = t
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
		return fmt.Errorf("error from nbd server: %s", err)
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

func nbdServeAction(cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		return torus.ErrUsage
	}

	srv := createServer()
	defer srv.Close()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	devfinder := &finder{srv}
	server, err := nbd.NewNBDServer(serveListenAddress, devfinder)
	if err != nil {
		return fmt.Errorf("can't start server: %v", err)
	}

	// TODO: sync all conns
	go func() {
		<-signalChan
		server.Close()
		return
	}()

	if err := server.Serve(); err != nil {
		return fmt.Errorf("server exited: %v", err)
	}
	return nil
}
