package main

import (
	"fmt"
	"io"
	"os"

	"github.com/coreos/agro/block"
	"github.com/spf13/cobra"
)

var dumpCommand = &cobra.Command{
	Use:   "dump VOLUME OUTPUT_FILE",
	Short: "dump the contents of a volume to an output file",
	Run:   dumpAction,
}

var loadCommand = &cobra.Command{
	Use:   "load INPUT_FILE VOLUME",
	Short: "load the contents of a file into a new volume",
	Run:   loadAction,
}

func init() {
	rootCommand.AddCommand(dumpCommand)
	rootCommand.AddCommand(loadCommand)
}

func dumpAction(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		os.Exit(1)
	}
	output, err := getWriterFromArg(args[1])
	if err != nil {
		die("couldn't open output: %v", err)
	}
	srv := createServer()
	blockvol, err := block.OpenBlockVolume(srv, args[0])
	if err != nil {
		die("couldn't open block volume %s: %v", args[0], err)
	}
	tempsnap := fmt.Sprintf("temp-dump-%d", os.Getpid())
	err = blockvol.SaveSnapshot(tempsnap)
	if err != nil {
		die("couldn't snapshot: %v", err)
	}
	bf, err := blockvol.OpenSnapshot(tempsnap)
	if err != nil {
		die("couldn't open snapshot: %v", err)
	}
	copied, err := io.Copy(output, bf)
	if err != nil {
		die("couldn't copy: %v", err)
	}
	err = blockvol.DeleteSnapshot(tempsnap)
	if err != nil {
		die("couldn't delete snapshot: %v", err)
	}
	fmt.Printf("copied %d bytes", copied)
}

func getReaderFromArg(arg string) (*os.File, error) {
	return os.OpenFile(arg, os.O_RDONLY, 0)
}

func getWriterFromArg(arg string) (io.Writer, error) {
	if arg == "-" {
		return os.Stdout, nil
	}
	return os.Create(arg)
}

func loadAction(cmd *cobra.Command, args []string) {

}
