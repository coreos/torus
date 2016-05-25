package main

import (
	"fmt"
	"net/url"
	"os"
	"os/signal"

	"github.com/coreos/pkg/capnslog"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"

	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/distributor"
	"github.com/coreos/agro/internal/http"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/ring"

	// Register all the possible drivers.
	_ "github.com/coreos/agro/block"
	_ "github.com/coreos/agro/metadata/etcd"
	_ "github.com/coreos/agro/metadata/temp"
	_ "github.com/coreos/agro/storage"
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
	debugInit        bool
	autojoin         bool
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
	rootCommand.PersistentFlags().BoolVarP(&debugInit, "debug-init", "", false, "Run a default init for the MDS if one doesn't exist")
	rootCommand.PersistentFlags().StringVarP(&etcdAddress, "etcd", "", "", "Address for talking to etcd")
	rootCommand.PersistentFlags().StringVarP(&host, "host", "", "127.0.0.1", "Host to listen on for HTTP")
	rootCommand.PersistentFlags().IntVarP(&port, "port", "", 4321, "Port to listen on for HTTP")
	rootCommand.PersistentFlags().StringVarP(&peerAddress, "peer-address", "", "", "Address to listen on for intra-cluster data")
	rootCommand.PersistentFlags().StringVarP(&sizeStr, "size", "", "1GiB", "How much disk space to use for this storage node")
	rootCommand.PersistentFlags().StringVarP(&readCacheSizeStr, "read-cache-size", "", "20MiB", "Amount of memory to use for read cache")
	rootCommand.PersistentFlags().StringVarP(&logpkg, "logpkg", "", "", "Specific package logging")
	rootCommand.PersistentFlags().StringVarP(&readLevel, "readlevel", "", "block", "Read replication level")
	rootCommand.PersistentFlags().StringVarP(&writeLevel, "writelevel", "", "all", "Write replication level")
	rootCommand.PersistentFlags().BoolVarP(&autojoin, "auto-join", "", false, "Automatically join the storage pool")
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
		srv *agro.Server
		err error
	)
	switch {
	case etcdAddress == "":
		srv, err = agro.NewServer(cfg, "temp", "mfile")
	case debugInit:
		err = agro.InitMDS("etcd", cfg, agro.GlobalMetadata{
			BlockSize:        512 * 1024,
			DefaultBlockSpec: blockset.MustParseBlockLayerSpec("crc,base"),
			INodeReplication: 2,
		}, ring.Ketama)
		if err != nil {
			if err == agro.ErrExists {
				fmt.Println("debug-init: Already exists")
			} else {
				fmt.Printf("Couldn't debug-init: %s\n", err)
				os.Exit(1)
			}
		}
		fallthrough
	default:
		srv, err = agro.NewServer(cfg, "etcd", "mfile")
	}
	if err != nil {
		fmt.Printf("Couldn't start: %s\n", err)
		os.Exit(1)
	}

	if autojoin {
		err = doAutojoin(srv)
		if err != nil {
			fmt.Printf("Couldn't auto-join: %s\n", err)
			os.Exit(1)
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	if peerAddress != "" {
		u, err := url.Parse(peerAddress)
		if err != nil {
			fmt.Printf("Couldn't parse peer address %s: %s\n", peerAddress, err)
			os.Exit(1)
		}
		err = distributor.ListenReplication(srv, u)
	} else {
		err = distributor.OpenReplication(srv)
	}

	defer srv.Close()
	go func() {
		for _ = range signalChan {
			fmt.Println("\nReceived an interrupt, stopping services...")
			os.Exit(0)
		}
	}()

	if err != nil {
		fmt.Println("couldn't use server as filesystem:", err)
		os.Exit(1)
	}
	http.ServeHTTP(httpAddress, srv)
}

func doAutojoin(s *agro.Server) error {
	for {
		ring, err := s.MDS.GetRing()
		if err != nil {
			fmt.Fprintf(os.Stderr, "couldn't get ring: %v\n", err)
			return err
		}
		var newRing agro.Ring
		if r, ok := ring.(agro.RingAdder); ok {
			newRing, err = r.AddPeers(agro.PeerInfoList{
				&models.PeerInfo{
					UUID:        s.MDS.UUID(),
					TotalBlocks: s.Blocks.NumBlocks(),
				},
			})
		} else {
			fmt.Fprintf(os.Stderr, "current ring type cannot support auto-adding\n")
			return err
		}
		if err == agro.ErrExists {
			// We're already a member; we're coming back up.
			return nil
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "couldn't add peer to ring: %v", err)
			return err
		}
		err = s.MDS.SetRing(newRing)
		if err == agro.ErrNonSequentialRing {
			continue
		}
		return err
	}
}
