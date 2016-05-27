package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/coreos/torus"
	_ "github.com/coreos/torus/metadata/etcd"
)

var (
	yesIAmSurePleaseWipe bool
)
var wipeCommand = &cobra.Command{
	Use:   "wipe",
	Short: "Remove all torus metadata from etcd",
	Run:   wipeAction,
}

func init() {
	wipeCommand.Flags().BoolVarP(&yesIAmSurePleaseWipe, "yes-i-am-sure", "", false, "progamatically wipe everything from the metadata store")
}

func wipeAction(cmd *cobra.Command, args []string) {
	if !yesIAmSurePleaseWipe {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("This will wipe all metadata for torus.\nPlease type `YES`, all caps to confirm: ")
		text, _ := reader.ReadString('\n')
		if text != "YES" {
			fmt.Println("`YES` not entered, exiting")
			os.Exit(1)
		}
	}
	cfg := torus.Config{
		MetadataAddress: etcdAddress,
	}
	err := torus.WipeMDS("etcd", cfg)
	if err != nil {
		die("error wiping metadata: %v", err)
	}
}
