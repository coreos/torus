package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	fsck "github.com/coreos/torus/cmd/fsck.torus/lib"
)

var (
	verbose      bool
	blockSizeStr string
)

func stderr(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, strings.TrimSpace(msg)+"\n", args...)
}

func debug(msg string, args ...interface{}) {
	if verbose {
		stderr(msg, args...)
	}
}

func die(why string, args ...interface{}) {
	stderr(why, args...)
	os.Exit(1)
}

func main() {
	if err := rootCommand.Execute(); err != nil {
		die("%v", err)
	}
}

var rootCommand = &cobra.Command{
	Use:   "fsck.torus device",
	Short: "check the consistency of a block device for torus",
	Long:  "",
	Run:   runFunc,
}

func init() {
	rootCommand.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "enable debug logging")
	rootCommand.PersistentFlags().StringVarP(&blockSizeStr, "block-size", "b", "512K", "torus cluster block size")
}

func runFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}

	torusBlockSize, err := parseBlockSize(blockSizeStr)
	if err != nil {
		die("error parsing block size: %v", err)
	}

	stderr("Checking the consistency of %s", args[0])

	fsck.Fsck(verbose, args[0], torusBlockSize)
}

func parseBlockSize(str string) (uint64, error) {
	str = strings.ToUpper(str)
	multiplier := 1
	switch {
	case strings.HasSuffix(str, "K"):
		multiplier = 1024
		str = strings.TrimSuffix(str, "K")
	case strings.HasSuffix(str, "M"):
		multiplier = 1024 * 1024
		str = strings.TrimSuffix(str, "M")
	case strings.HasSuffix(str, "G"):
		multiplier = 1024 * 1024 * 1024 // I really really hope no one uses this
		str = strings.TrimSuffix(str, "G")
	}
	blockSize, err := strconv.ParseUint(str, 0, 64)
	if err != nil {
		return 0, err
	}
	return blockSize * uint64(multiplier), nil
}
