package main

import (
	"fmt"
	"os"

	"github.com/coreos/pkg/capnslog"
	"github.com/spf13/cobra"
)

var etcdAddress string

var rootCommand = &cobra.Command{
	Use:   "agroctl",
	Short: "Administer the agro filesystem",
	Long:  `Admin utility for the agro distributed filesystem.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
		os.Exit(1)
	},
}

func init() {
	rootCommand.PersistentFlags().StringVarP(&etcdAddress, "etcd", "C", "127.0.0.1:2378", "hostname:port to the etcd instance storing the metadata")
	rootCommand.AddCommand(mkfsCommand)
	rootCommand.AddCommand(listPeersCommand)
	rootCommand.AddCommand(ringCommand)
	rootCommand.AddCommand(peerCommand)
}

func main() {
	capnslog.SetGlobalLogLevel(capnslog.WARNING)

	if err := rootCommand.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
