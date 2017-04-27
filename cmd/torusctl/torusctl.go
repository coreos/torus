package main

import (
	"fmt"
	"os"

	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/torus"
	"github.com/coreos/torus/internal/flagconfig"
	"github.com/spf13/cobra"
)

var (
	debug bool
)

var rootCommand = &cobra.Command{
	Use:              "torusctl",
	Short:            "Administer the torus storage cluster",
	Long:             `Admin utility for the torus distributed storage cluster.`,
	PersistentPreRun: configure,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
		os.Exit(1)
	},
}

var versionCommand = &cobra.Command{
	Use:   "version",
	Short: "print version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("torusctl\nVersion: %s\n", torus.Version)
		os.Exit(0)
	},
}

func init() {
	fmt.Println("start to init MDS...")
	rootCommand.PersistentFlags().BoolVarP(&debug, "debug", "", false, "enable debug logging")
	rootCommand.AddCommand(initCommand)
	rootCommand.AddCommand(blockCommand)
	//	rootCommand.AddCommand(listPeersCommand)
	rootCommand.AddCommand(ringCommand)
	rootCommand.AddCommand(peerCommand)
	rootCommand.AddCommand(volumeCommand)
	rootCommand.AddCommand(versionCommand)
	rootCommand.AddCommand(wipeCommand)
	rootCommand.AddCommand(configCommand)
	rootCommand.AddCommand(completionCommand)
	flagconfig.AddConfigFlags(rootCommand.PersistentFlags())

	fmt.Println("MDS init finished...")
}

func main() {
	if err := rootCommand.Execute(); err != nil {
		die("%v", err)
	}
}

func configure(cmd *cobra.Command, args []string) {
	capnslog.SetGlobalLogLevel(capnslog.WARNING)

	if debug {
		capnslog.SetGlobalLogLevel(capnslog.DEBUG)
	}
}
