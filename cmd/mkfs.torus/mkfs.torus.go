package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/coreos/torus/cmd/mkfs.torus/lib"
)

var (
	verbose      bool
	assumeYes    bool
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
	Use:   "mkfs.torus device",
	Short: "format a block device for torus",
	Long:  "",
	Run:   runFunc,
}

func init() {
	rootCommand.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "enable debug logging")
	rootCommand.PersistentFlags().BoolVarP(&assumeYes, "yes", "y", false, "assume yes on all prompts")
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

	stderr("This is going to format %s for torus.", args[0])
	stderr("This will destroy any data on the device.")
	if !assumeYes {
		fmt.Fprintf(os.Stderr, "Are you SURE? [y/N] ")

		var response string
		_, err = fmt.Scanln(&response)
		if err != nil {
			// Not providing input can error, so not worth printing it
			os.Exit(1)
		}

		userSaidYes := false

		for _, s := range []string{"y", "yes"} {
			userSaidYes = userSaidYes || strings.Compare(strings.ToLower(response), s) == 0
		}

		if !userSaidYes {
			os.Exit(1)
		}
	}

	err = lib.Mkfs(torusBlockSize, verbose, args[0])
	if err != nil {
		die("%v", err)
	}
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
