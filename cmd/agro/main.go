package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/coreos/pkg/capnslog"
	"github.com/spf13/cobra"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/blockset"
	"github.com/barakmich/agro/internal/http"
	"github.com/barakmich/agro/server"

	// Register all the possible drivers.
	_ "github.com/barakmich/agro/metadata/etcd"
	_ "github.com/barakmich/agro/metadata/temp"
	_ "github.com/barakmich/agro/storage/block"
	_ "github.com/barakmich/agro/storage/inode"
)

var (
	dataDir     string
	etcdAddress string
	httpAddress string
	port        int
	mkfs        bool

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
	rootCommand.PersistentFlags().IntVarP(&port, "port", "", 4321, "Port to listen on for HTTP")
	rootCommand.PersistentFlags().BoolVarP(&trace, "trace", "", false, "Turn on trace output")
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

	httpAddress = fmt.Sprintf("127.0.0.1:%d", port)
}

func runServer(cmd *cobra.Command, args []string) {
	cfg := agro.Config{
		DataDir:         dataDir,
		StorageSize:     1 * 1024 * 1024 * 1024,
		MetadataAddress: etcdAddress,
	}

	var (
		srv agro.Server
		err error
	)
	switch {
	case etcdAddress == "":
		srv, err = server.NewServer(cfg, "temp", "bolt", "mfile")
	case mkfs:
		err = agro.Mkfs("etcd", cfg, agro.GlobalMetadata{
			BlockSize:        8 * 1024,
			DefaultBlockSpec: blockset.MustParseBlockLayerSpec("crc,base"),
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
		srv, err = server.NewServer(cfg, "etcd", "bolt", "mfile")
	}
	if err != nil {
		fmt.Printf("Couldn't start: %s\n", err)
		os.Exit(1)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	srv.BeginHeartbeat()

	go func() {
		for _ = range signalChan {
			fmt.Println("\nReceived an interrupt, stopping services...")
			srv.Close()
			os.Exit(0)
		}
	}()

	http.ServeHTTP(httpAddress, srv)
}
