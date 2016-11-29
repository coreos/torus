package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/coreos/torus"
	"github.com/coreos/torus/block"
	"github.com/coreos/torus/block/aoe"
)

const (
	flushDevice = "/dev/etherd/flush"
)

var aoeCommand = &cobra.Command{
	Use:   "aoe VOLUME INTERFACE MAJOR MINOR",
	Short: "serve a volume over AoE",
	Long: strings.TrimSpace(`
Serve a volume over AoE using the specified network interface and AoE
major and minor addresses.

It is important to note that all AoE servers on the same layer 2 network
must have different major and minor addresses.

An example of serving two volumes using AoE on the same server over the
eth0 network interface:

	torusblk aoe vol01 eth0 1 1
	torusblk aoe vol02 eth0 1 2
`),
	Run: func(cmd *cobra.Command, args []string) {
		err := aoeAction(cmd, args)
		if err == torus.ErrUsage {
			cmd.Usage()
			os.Exit(1)
		} else if err != nil {
			die("%v", err)
		}
	},
}

var (
	aoeFlush string
)

func init() {
	aoeCommand.Flags().StringVarP(&aoeFlush, "flush", "", "", "flush AOE device (e.g. torsublk aoe --flush e1.1)")
}

func aoeAction(cmd *cobra.Command, args []string) error {
	if len(aoeFlush) > 0 && len(args) == 0 {
		if err := flush(aoeFlush); err != nil {
			return fmt.Errorf("failed to flush: %v", err)
		}
		return nil
	}

	if len(args) != 4 {
		return torus.ErrUsage
	}

	srv := createServer()

	vol := args[0]
	ifname := args[1]
	maj := args[2]
	min := args[3]

	major, err := strconv.ParseUint(maj, 10, 16)
	if err != nil {
		return fmt.Errorf("Failed to parse major address %q: %v", maj, err)
	}

	minor, err := strconv.ParseUint(min, 10, 8)
	if err != nil {
		return fmt.Errorf("Failed to parse minor address %q: %v", min, err)
	}

	blockvol, err := block.OpenBlockVolume(srv, vol)
	if err != nil {
		return fmt.Errorf("server doesn't support block volumes: %v", err)
	}

	ai, err := aoe.NewInterface(ifname)
	if err != nil {
		return fmt.Errorf("Failed to set up interface %q: %v", ifname, err)
	}

	as, err := aoe.NewServer(blockvol, &aoe.ServerOptions{
		Major: uint16(major),
		Minor: uint8(minor),
	})
	if err != nil {
		return fmt.Errorf("Failed to crate AoE server: %v", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func(sv *torus.Server, iface *aoe.Interface) {
		for _ = range signalChan {
			fmt.Println("\nReceived an interrupt, stopping services...")

			iface.Close()
			sv.Close()
			as.Close()
			os.Exit(0)
		}
	}(srv, ai)

	if err = as.Serve(ai); err != nil {
		return fmt.Errorf("Failed to serve AoE: %v", err)
	}
	return nil
}

func flush(d string) error {
	fd, err := os.OpenFile(flushDevice, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()
	writer := bufio.NewWriter(fd)
	_, err = writer.WriteString(d)
	if err != nil {
		return err
	}
	writer.Flush()
	return nil
}
