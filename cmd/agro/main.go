package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/coreos/pkg/capnslog"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"

	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/internal/http"
	"github.com/coreos/agro/ring"
	"github.com/coreos/agro/server"

	// Register all the possible drivers.
	_ "github.com/coreos/agro/metadata/etcd"
	_ "github.com/coreos/agro/metadata/temp"
	_ "github.com/coreos/agro/storage/block"
)

var (
	dataDir          string
	etcdAddress      string
	httpAddress      string
	peerAddress      string
	readCacheSize    uint64
	readCacheSizeStr string
	sizeStr          string
	size             uint64
	host             string
	port             int
	mkfs             bool
	logpkg           string
	readLevel        string
	writeLevel       string
	cfg              agro.Config

	debug bool
)

var rootCommand = &cobra.Command{
	Use:    "agro",
	Short:  "Agro distributed filesystem",
	Long:   `The agro distributed filesystem server.`,
	PreRun: configureServer,
	Run:    runServer,
}

func init() {
	rootCommand.PersistentFlags().StringVarP(&dataDir, "datadir", "", "/tmp/agro", "Path to the data directory")
	rootCommand.PersistentFlags().BoolVarP(&debug, "debug", "", false, "Turn on debug output")
	rootCommand.PersistentFlags().BoolVarP(&mkfs, "debug-mkfs", "", false, "Run mkfs for the given ")
	rootCommand.PersistentFlags().StringVarP(&etcdAddress, "etcd", "", "", "Address for talking to etcd")
	rootCommand.PersistentFlags().StringVarP(&host, "host", "", "127.0.0.1", "Host to listen on for HTTP")
	rootCommand.PersistentFlags().IntVarP(&port, "port", "", 4321, "Port to listen on for HTTP")
	rootCommand.PersistentFlags().StringVarP(&peerAddress, "peer-address", "", "", "Address to listen on for intra-cluster data")
	rootCommand.PersistentFlags().StringVarP(&sizeStr, "size", "", "1GiB", "How much disk space to use for this storage node")
	rootCommand.PersistentFlags().StringVarP(&readCacheSizeStr, "read-cache-size", "", "20MiB", "Amount of memory to use for read cache")
	rootCommand.PersistentFlags().StringVarP(&logpkg, "logpkg", "", "", "Specific package logging")
	rootCommand.PersistentFlags().StringVarP(&readLevel, "readlevel", "", "block", "Read replication level")
	rootCommand.PersistentFlags().StringVarP(&writeLevel, "writelevel", "", "all", "Write replication level")
}

func main() {
	if err := rootCommand.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
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
		rl := capnslog.MustRepoLogger("github.com/coreos/agro")
		llc, err := rl.ParseLogLevelConfig(logpkg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error parsing logpkg: %s\n", err)
			os.Exit(1)
		}
		rl.SetLogLevel(llc)
	}

	httpAddress = fmt.Sprintf("%s:%d", host, port)

	var err error
	readCacheSize, err = humanize.ParseBytes(readCacheSizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing read-cache-size: %s\n", err)
		os.Exit(1)
	}
	size, err = humanize.ParseBytes(sizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing size: %s\n", err)
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

	var wl agro.WriteLevel
	switch writeLevel {
	case "all":
		wl = agro.WriteAll
	case "one":
		wl = agro.WriteOne
	case "local":
		wl = agro.WriteLocal
	default:
		fmt.Fprintf(os.Stderr, "invalid writelevel; use one of 'one', 'all', or 'local'")
		os.Exit(1)
	}
	cfg = agro.Config{
		DataDir:         dataDir,
		StorageSize:     size,
		MetadataAddress: etcdAddress,
		ReadCacheSize:   readCacheSize,
		WriteLevel:      wl,
		ReadLevel:       rl,
	}
}

func runServer(cmd *cobra.Command, args []string) {

	var (
		srv agro.Server
		err error
	)
	switch {
	case etcdAddress == "":
		srv, err = server.NewServer(cfg, "temp", "mfile")
	case mkfs:
		err = agro.Mkfs("etcd", cfg, agro.GlobalMetadata{
			BlockSize:        8 * 1024,
			DefaultBlockSpec: blockset.MustParseBlockLayerSpec("crc,base"),
			INodeReplication: 3,
		}, ring.Ketama)
		if err != nil {
			if err == agro.ErrExists {
				fmt.Println("debug-mkfs: Already exists")
			} else {
				fmt.Printf("Couldn't debug-mkfs: %s\n", err)
				os.Exit(1)
			}
		}
		fallthrough
	default:
		srv, err = server.NewServer(cfg, "etcd", "mfile")
	}
	if err != nil {
		fmt.Printf("Couldn't start: %s\n", err)
		os.Exit(1)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	if peerAddress != "" {
		srv.ListenReplication(peerAddress)
	} else {
		srv.OpenReplication()
	}

	go func() {
		for _ = range signalChan {
			fmt.Println("\nReceived an interrupt, stopping services...")
			srv.Close()
			os.Exit(0)
		}
	}()

	http.ServeHTTP(httpAddress, srv)
}
