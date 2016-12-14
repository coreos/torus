package main

import (
	"fmt"
	"os"

	"github.com/coreos/pkg/capnslog"
	"github.com/spf13/cobra"

	"github.com/coreos/torus"
	"github.com/coreos/torus/distributor"
	"github.com/coreos/torus/internal/flagconfig"
	"github.com/coreos/torus/internal/http"

	// Register all the drivers.
	_ "github.com/coreos/torus/metadata/etcd"
	_ "github.com/coreos/torus/storage"
)

var (
	logpkg   string
	httpAddr string
	cfg      torus.Config

	debug bool
)

var rootCommand = &cobra.Command{
	Use:              "torusblk",
	Short:            "torus block volume tool",
	Long:             "Control block volumes on the torus distributed storage system",
	PersistentPreRun: configureServer,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
		os.Exit(1)
	},
}

var versionCommand = &cobra.Command{
	Use:   "version",
	Short: "print version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("torusblk\nVersion: %s\n", torus.Version)
		os.Exit(0)
	},
}

func init() {
	rootCommand.AddCommand(aoeCommand)
	rootCommand.AddCommand(versionCommand)
	rootCommand.AddCommand(completionCommand)

	// Flexvolume commands
	rootCommand.AddCommand(initCommand)
	rootCommand.AddCommand(attachCommand)
	rootCommand.AddCommand(detachCommand)
	rootCommand.AddCommand(mountCommand)
	rootCommand.AddCommand(unmountCommand)
	rootCommand.AddCommand(flexprepvolCommand)

	rootCommand.PersistentFlags().StringVarP(&logpkg, "logpkg", "", "", "Specific package logging")
	rootCommand.PersistentFlags().StringVarP(&httpAddr, "http", "", "", "HTTP endpoint for debug and stats")
	rootCommand.PersistentFlags().BoolVarP(&debug, "debug", "", false, "Turn on debug output")
	flagconfig.AddConfigFlags(rootCommand.PersistentFlags())
}

func configureServer(cmd *cobra.Command, args []string) {
	switch {
	case debug:
		capnslog.SetGlobalLogLevel(capnslog.DEBUG)
	default:
		capnslog.SetGlobalLogLevel(capnslog.INFO)
	}
	if logpkg != "" {
		capnslog.SetGlobalLogLevel(capnslog.NOTICE)
		rl := capnslog.MustRepoLogger("github.com/coreos/torus")
		llc, err := rl.ParseLogLevelConfig(logpkg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error parsing logpkg: %s\n", err)
			os.Exit(1)
		}
		rl.SetLogLevel(llc)
	}

	cfg = flagconfig.BuildConfigFromFlags()
}

func createServer() *torus.Server {
	srv, err := torus.NewServer(cfg, "etcd", "temp")
	if err != nil {
		fmt.Printf("couldn't start: %s\n", err)
		os.Exit(1)
	}
	err = distributor.OpenReplication(srv)
	if err != nil {
		fmt.Printf("couldn't start: %s", err)
		os.Exit(1)
	}
	if httpAddr != "" {
		go http.ServeHTTP(httpAddr, srv)
	}
	return srv
}

func main() {
	capnslog.SetGlobalLogLevel(capnslog.WARNING)

	if err := rootCommand.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func die(why string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, why+"\n", args...)
	os.Exit(1)
}
