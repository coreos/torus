package main

import (
	"fmt"
	"os"

	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/torus"
	"github.com/spf13/cobra"
)

var etcdAddress string

var rootCommand = &cobra.Command{
	Use:   "torusctl",
	Short: "Administer the torus storage cluster",
	Long:  `Admin utility for the torus distributed storage cluster.`,
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
	rootCommand.AddCommand(initCommand)
	rootCommand.AddCommand(listPeersCommand)
	rootCommand.AddCommand(ringCommand)
	rootCommand.AddCommand(peerCommand)
	rootCommand.AddCommand(volumeCommand)
	rootCommand.AddCommand(versionCommand)
}

func main() {
	capnslog.SetGlobalLogLevel(capnslog.WARNING)

	if err := rootCommand.Execute(); err != nil {
		die("%v", err)
	}
}
