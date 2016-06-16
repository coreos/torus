package main

import (
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
	Run: aoeAction,
}

func aoeAction(cmd *cobra.Command, args []string) {
	if len(args) != 4 {
		cmd.Usage()
		os.Exit(1)
	}

	srv := createServer()

	vol := args[0]
	ifname := args[1]
	maj := args[2]
	min := args[3]

	major, err := strconv.ParseUint(maj, 10, 16)
	if err != nil {
		die("Failed to parse major address %q: %v\n", maj, err)
	}

	minor, err := strconv.ParseUint(min, 10, 8)
	if err != nil {
		die("Failed to parse minor address %q: %v\n", min, err)
	}

	blockvol, err := block.OpenBlockVolume(srv, vol)
	if err != nil {
		fmt.Println("server doesn't support block volumes:", err)
		os.Exit(1)
	}

	ai, err := aoe.NewInterface(ifname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to set up interface %q: %v\n", ifname, err)
		os.Exit(1)
	}

	as, err := aoe.NewServer(blockvol, &aoe.ServerOptions{
		Major: uint16(major),
		Minor: uint8(minor),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to crate AoE server: %v\n", err)
		os.Exit(1)
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
		fmt.Fprintf(os.Stderr, "Failed to serve AoE: %v\n", err)
		os.Exit(1)
	}
}
