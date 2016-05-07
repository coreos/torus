package main

import (
	"fmt"
	"os"

	"github.com/coreos/pkg/capnslog"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"

	"github.com/coreos/agro"

	// Register all the drivers.
	"github.com/coreos/agro/distributor"
	"github.com/coreos/agro/internal/http"
	_ "github.com/coreos/agro/metadata/etcd"
	_ "github.com/coreos/agro/storage/block"
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

	cfg agro.Config
)

var rootCommand = &cobra.Command{
	Use:              "agromount",
	Short:            "Mount volumes from the agro filesystem",
	Long:             `Daemon to mount volumes from the agro distributed filesystem.`,
	PersistentPreRun: configureServer,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
		os.Exit(1)
	},
}

func init() {
	rootCommand.AddCommand(aoeCommand)
	rootCommand.AddCommand(nbdCommand)
	rootCommand.AddCommand(initCommand)
	rootCommand.AddCommand(attachCommand)
	rootCommand.AddCommand(detachCommand)
	rootCommand.AddCommand(mountCommand)
	rootCommand.AddCommand(unmountCommand)
	rootCommand.AddCommand(flexprepvolCommand)

	rootCommand.PersistentFlags().StringVarP(&etcdAddress, "etcd", "C", "127.0.0.1:2378", "hostname:port to the etcd instance storing the metadata")
	rootCommand.PersistentFlags().StringVarP(&localBlockSizeStr, "write-cache-size", "", "128MiB", "Amount of memory to use for the local write cache")
	rootCommand.PersistentFlags().StringVarP(&readCacheSizeStr, "read-cache-size", "", "20MiB", "Amount of memory to use for read cache")
	rootCommand.PersistentFlags().StringVarP(&logpkg, "logpkg", "", "", "Specific package logging")
	rootCommand.PersistentFlags().StringVarP(&readLevel, "read-level", "", "block", "Read replication level")
	rootCommand.PersistentFlags().StringVarP(&writeLevel, "write-level", "", "all", "Write replication level")
	rootCommand.PersistentFlags().StringVarP(&httpAddr, "http", "", "", "HTTP endpoint for debug and stats")
}

func configureServer(cmd *cobra.Command, args []string) {
	capnslog.SetGlobalLogLevel(capnslog.NOTICE)
	if logpkg != "" {
		rl := capnslog.MustRepoLogger("github.com/coreos/agro")
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

	var rl agro.ReadLevel
	switch readLevel {
	case "spread":
		rl = agro.ReadSpread
	case "seq":
		rl = agro.ReadSequential
	case "block":
		rl = agro.ReadBlock
	default:
		fmt.Fprintf(os.Stderr, "invalid readlevel; use one of 'spread', 'seq', or 'block'")
		os.Exit(1)
	}

	wl, err := agro.ParseWriteLevel(writeLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	cfg = agro.Config{
		StorageSize:     localBlockSize,
		MetadataAddress: etcdAddress,
		ReadCacheSize:   readCacheSize,
		WriteLevel:      wl,
		ReadLevel:       rl,
	}
}

func createServer() *agro.Server {
	srv, err := agro.NewServer(cfg, "etcd", "temp")
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
