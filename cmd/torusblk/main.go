package main

import (
	"fmt"
	"os"

	"github.com/coreos/pkg/capnslog"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"

	"github.com/coreos/torus"
	"github.com/coreos/torus/distributor"
	"github.com/coreos/torus/internal/http"

	// Register all the drivers.
	_ "github.com/coreos/torus/metadata/etcd"
	_ "github.com/coreos/torus/storage"
)

var (
	etcdAddress       string
	localBlockSizeStr string
	localBlockSize    uint64
	readCacheSizeStr  string
	readCacheSize     uint64
	readLevel         string
	writeLevel        string
	logpkg            string
	httpAddr          string

	cfg torus.Config
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
	rootCommand.AddCommand(nbdCommand)
	rootCommand.AddCommand(volumeCommand)
	rootCommand.AddCommand(versionCommand)

	// Flexvolume commands
	rootCommand.AddCommand(initCommand)
	rootCommand.AddCommand(attachCommand)
	rootCommand.AddCommand(detachCommand)
	rootCommand.AddCommand(mountCommand)
	rootCommand.AddCommand(unmountCommand)
	rootCommand.AddCommand(flexprepvolCommand)

	rootCommand.PersistentFlags().StringVarP(&etcdAddress, "etcd", "C", "127.0.0.1:2379", "hostname:port to the etcd instance storing the metadata")
	rootCommand.PersistentFlags().StringVarP(&localBlockSizeStr, "write-cache-size", "", "128MiB", "Maximum amount of memory to use for the local write cache")
	rootCommand.PersistentFlags().StringVarP(&readCacheSizeStr, "read-cache-size", "", "50MiB", "Amount of memory to use for read cache")
	rootCommand.PersistentFlags().StringVarP(&logpkg, "logpkg", "", "", "Specific package logging")
	rootCommand.PersistentFlags().StringVarP(&readLevel, "read-level", "", "block", "Read replication level")
	rootCommand.PersistentFlags().StringVarP(&writeLevel, "write-level", "", "all", "Write replication level")
	rootCommand.PersistentFlags().StringVarP(&httpAddr, "http", "", "", "HTTP endpoint for debug and stats")
}

func configureServer(cmd *cobra.Command, args []string) {
	capnslog.SetGlobalLogLevel(capnslog.NOTICE)
	if logpkg != "" {
		rl := capnslog.MustRepoLogger("github.com/coreos/torus")
		llc, err := rl.ParseLogLevelConfig(logpkg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error parsing logpkg: %s\n", err)
			os.Exit(1)
		}
		rl.SetLogLevel(llc)
	}

	var err error
	readCacheSize, err = humanize.ParseBytes(readCacheSizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing read-cache-size: %s\n", err)
		os.Exit(1)
	}
	localBlockSize, err = humanize.ParseBytes(localBlockSizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing write-cache-size: %s\n", err)
		os.Exit(1)
	}

	var rl torus.ReadLevel
	switch readLevel {
	case "spread":
		rl = torus.ReadSpread
	case "seq":
		rl = torus.ReadSequential
	case "block":
		rl = torus.ReadBlock
	default:
		fmt.Fprintf(os.Stderr, "invalid readlevel; use one of 'spread', 'seq', or 'block'")
		os.Exit(1)
	}

	wl, err := torus.ParseWriteLevel(writeLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	cfg = torus.Config{
		StorageSize:     localBlockSize,
		MetadataAddress: etcdAddress,
		ReadCacheSize:   readCacheSize,
		WriteLevel:      wl,
		ReadLevel:       rl,
	}
}

func createServer() *torus.Server {
	srv, err := torus.NewServer(cfg, "etcd", "temp")
	if err != nil {
		fmt.Printf("Couldn't start: %s\n", err)
		os.Exit(1)
	}
	err = distributor.OpenReplication(srv)
	if err != nil {
		fmt.Printf("Couldn't start: %s", err)
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
