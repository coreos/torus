package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/spf13/cobra"

	"github.com/coreos/agro"
	"github.com/coreos/agro/server/aoe"
)

var aoeCommand = &cobra.Command{
	Use:   "aoe VOLUME INTERFACE",
	Short: "serve a volume over AoE",
	Run:   aoeAction,
}

func aoeAction(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		os.Exit(1)
	}

	srv := createServer()

	blocksrv, err := srv.Block()
	if err != nil {
		fmt.Println("server doesn't support block volumes:", err)
		os.Exit(1)
	}

	vol := args[0]
	ifname := args[1]

	ai, err := aoe.NewInterface(ifname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to set up interface %q: %v\n", ifname, err)
		os.Exit(1)
	}

	as, err := aoe.NewServer(blocksrv, vol)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to crate AoE server: %v\n", err)
		os.Exit(1)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func(sv agro.BlockServer, iface *aoe.Interface) {
		for _ = range signalChan {
			fmt.Println("\nReceived an interrupt, stopping services...")

			iface.Close()
			sv.Close()
			as.Close()
			os.Exit(0)
		}
	}(blocksrv, ai)

	if err = as.Serve(ai); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to serve AoE: %v\n", err)
		os.Exit(1)
	}
}
