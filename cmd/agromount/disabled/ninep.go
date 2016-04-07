package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/spf13/cobra"

	"github.com/coreos/agro/server/ninep"
)

var (
	ninepCommand = &cobra.Command{
		Use:   "ninep ADDRESS",
		Short: "Start a 9p listener for FS volumes",
		Run:   ninepAction,
	}
)

func init() {
}

func ninepAction(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}

	srv := createServer()
	defer srv.Close()
	addr := args[0]
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func() {
		for _ = range signalChan {
			fmt.Println("\nReceived an interrupt, stopping services...")
			os.Exit(0)
		}
	}()

	fsSrv, err := srv.FS()
	if err != nil {
		fmt.Println("server doesn't support filesystems:", err)
		os.Exit(1)
	}

	// TODO(mischief): allow clean termination
	if err := ninep.ListenAndServe(addr, fsSrv); err != nil {
		fmt.Printf("Listener died: %v", err)
		os.Exit(1)
	}
}
