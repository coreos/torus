package main

import (
	"fmt"
	"os"

	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/torus"
	"github.com/spf13/cobra"
)

var (
	etcdAddress string
	debug       bool
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
	rootCommand.PersistentFlags().StringVarP(&etcdAddress, "etcd", "C", "127.0.0.1:2379", "hostname:port to the etcd instance storing the metadata")
	rootCommand.PersistentFlags().BoolVarP(&debug, "debug", "", false, "enable debug logging")
	rootCommand.AddCommand(initCommand)
	rootCommand.AddCommand(listPeersCommand)
	rootCommand.AddCommand(ringCommand)
	rootCommand.AddCommand(peerCommand)
	rootCommand.AddCommand(volumeCommand)
	rootCommand.AddCommand(versionCommand)
	rootCommand.AddCommand(wipeCommand)
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
