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
	"github.com/coreos/agro/server"
	agrofuse "github.com/coreos/agro/server/fuse"

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
	fuseMountpoint   string
	fuseVolume       string
	readCacheSize    uint64
	readCacheSizeStr string
	sizeStr          string
	size             uint64
	host             string
	port             int
	mkfs             bool

	debug bool
	trace bool
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
	rootCommand.PersistentFlags().BoolVarP(&trace, "trace", "", false, "Turn on trace output")
	rootCommand.PersistentFlags().StringVarP(&peerAddress, "peer-address", "", "", "Address to listen on for intra-cluster data")
	rootCommand.PersistentFlags().StringVarP(&fuseMountpoint, "fuse-mountpoint", "", "", "Location to mount a FUSE filesystem")
	rootCommand.PersistentFlags().StringVarP(&fuseVolume, "fuse-volume", "", "", "Volume to be mounted as a FUSE filesystem")
	rootCommand.PersistentFlags().StringVarP(&sizeStr, "size", "", "1GiB", "Amount of memory to use for read cache")
	rootCommand.PersistentFlags().StringVarP(&readCacheSizeStr, "read-cache-size", "", "20MiB", "Amount of memory to use for read cache")
}

func main() {
	if err := rootCommand.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func configureServer(cmd *cobra.Command, args []string) {
	switch {
	case trace:
		capnslog.SetGlobalLogLevel(capnslog.TRACE)
	case debug:
		capnslog.SetGlobalLogLevel(capnslog.DEBUG)
	default:
		capnslog.SetGlobalLogLevel(capnslog.INFO)
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
}

func runServer(cmd *cobra.Command, args []string) {
	cfg := agro.Config{
		DataDir:         dataDir,
		StorageSize:     size,
		MetadataAddress: etcdAddress,
		ReadCacheSize:   readCacheSize,
	}

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
		})
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

	if fuseMountpoint != "" && fuseVolume != "" {
		if httpAddress != "" {
			go http.ServeHTTP(httpAddress, srv)
		}
		agrofuse.MustMount(fuseMountpoint, fuseVolume, srv)
	} else {
		http.ServeHTTP(httpAddress, srv)
	}
}
