package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/coreos/pkg/capnslog"
	"github.com/dustin/go-humanize"
	"github.com/ricochet2200/go-disk-usage/du"
	"github.com/spf13/cobra"

	"github.com/coreos/torus"
	"github.com/coreos/torus/blockset"
	"github.com/coreos/torus/distributor"
	"github.com/coreos/torus/internal/http"
	"github.com/coreos/torus/models"
	"github.com/coreos/torus/ring"

	// Register all the possible drivers.
	_ "github.com/coreos/torus/block"
	_ "github.com/coreos/torus/metadata/etcd"
	_ "github.com/coreos/torus/metadata/temp"
	_ "github.com/coreos/torus/storage"
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
	cfg              torus.Config

	debug   bool
	version bool
)

var rootCommand = &cobra.Command{
	Use:    "torusd",
	Short:  "Torus distributed storage",
	Long:   `The torus distributed storage server.`,
	PreRun: configureServer,
	Run:    runServer,
}

func init() {
	rootCommand.PersistentFlags().StringVarP(&dataDir, "data-dir", "", "", "Path to the data directory")
	rootCommand.PersistentFlags().BoolVarP(&debug, "debug", "", false, "Turn on debug output")
	rootCommand.PersistentFlags().BoolVarP(&debugInit, "debug-init", "", false, "Run a default init for the MDS if one doesn't exist")
	rootCommand.PersistentFlags().StringVarP(&etcdAddress, "etcd", "C", "", "Address for talking to etcd")
	rootCommand.PersistentFlags().StringVarP(&host, "host", "", "", "Host to listen on for HTTP")
	rootCommand.PersistentFlags().IntVarP(&port, "port", "", 4321, "Port to listen on for HTTP")
	rootCommand.PersistentFlags().StringVarP(&peerAddress, "peer-address", "", "", "Address to listen on for intra-cluster data")
	rootCommand.PersistentFlags().StringVarP(&sizeStr, "size", "", "1GiB", "How much disk space to use for this storage node")
	rootCommand.PersistentFlags().StringVarP(&readCacheSizeStr, "read-cache-size", "", "20MiB", "Amount of memory to use for read cache")
	rootCommand.PersistentFlags().StringVarP(&logpkg, "logpkg", "", "", "Specific package logging")
	rootCommand.PersistentFlags().StringVarP(&readLevel, "readlevel", "", "block", "Read replication level")
	rootCommand.PersistentFlags().StringVarP(&writeLevel, "writelevel", "", "all", "Write replication level")
	rootCommand.PersistentFlags().BoolVarP(&autojoin, "auto-join", "", false, "Automatically join the storage pool")
	rootCommand.PersistentFlags().BoolVarP(&version, "version", "", false, "Print version info and exit")
}

func main() {
	if err := rootCommand.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func configureServer(cmd *cobra.Command, args []string) {
	if version {
		fmt.Printf("torusd\nVersion: %s\n", torus.Version)
		os.Exit(0)
	}
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

	if host != "" {
		httpAddress = fmt.Sprintf("%s:%d", host, port)
	}

	var err error
	readCacheSize, err = humanize.ParseBytes(readCacheSizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing read-cache-size: %s\n", err)
		os.Exit(1)
	}
	if strings.Contains(sizeStr, "%") {

		percent, err := parsePercentage(sizeStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error parsing size: %s\n", err)
			os.Exit(1)
		}

		directory := dataDir
		if dataDir == "" {
			directory, _ = os.Getwd()
		} else {
			directory, _ = filepath.Abs(dataDir)
		}

		size = du.NewDiskUsage(directory).Size() * percent / 100

	} else {
		size, err = humanize.ParseBytes(sizeStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error parsing size: %s\n", err)
			os.Exit(1)
		}
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

	var wl torus.WriteLevel
	switch writeLevel {
	case "all":
		wl = torus.WriteAll
	case "one":
		wl = torus.WriteOne
	case "local":
		wl = torus.WriteLocal
	default:
		fmt.Fprintf(os.Stderr, "invalid writelevel; use one of 'one', 'all', or 'local'")
		os.Exit(1)
	}
	cfg = torus.Config{
		DataDir:         dataDir,
		StorageSize:     size,
		MetadataAddress: etcdAddress,
		ReadCacheSize:   readCacheSize,
		WriteLevel:      wl,
		ReadLevel:       rl,
	}
}

func parsePercentage(percentString string) (uint64, error) {
	sizePercent := strings.Split(percentString, "%")[0]
	sizeNumber, err := strconv.Atoi(sizePercent)
	if err != nil {
		return 0, err
	}
	if sizeNumber < 1 || sizeNumber > 100 {
		return 0, errors.New("invalid size; must be between 1%% and 100%%\n")
	}
	return uint64(sizeNumber), nil
}

func runServer(cmd *cobra.Command, args []string) {

	var (
		srv *torus.Server
		err error
	)
	switch {
	case etcdAddress == "":
		srv, err = torus.NewServer(cfg, "temp", "mfile")
	case debugInit:
		err = torus.InitMDS("etcd", cfg, torus.GlobalMetadata{
			BlockSize:        512 * 1024,
			DefaultBlockSpec: blockset.MustParseBlockLayerSpec("crc,base"),
			INodeReplication: 2,
		}, ring.Ketama)
		if err != nil {
			if err == torus.ErrExists {
				fmt.Println("debug-init: Already exists")
			} else {
				fmt.Printf("Couldn't debug-init: %s\n", err)
				os.Exit(1)
			}
		}
		fallthrough
	default:
		srv, err = torus.NewServer(cfg, "etcd", "mfile")
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

	mainClose := make(chan bool)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	if peerAddress != "" {
		var u *url.URL

		u, err = url.Parse(peerAddress)
		if err != nil {
			fmt.Printf("Couldn't parse peer address %s: %s\n", peerAddress, err)
			os.Exit(1)
		}

		if u.Scheme == "" {
			fmt.Printf("Peer address %s does not have URL scheme (http:// or tdp://)\n", peerAddress)
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
			close(mainClose)
			os.Exit(0)
		}
	}()

	if err != nil {
		fmt.Println("couldn't use server:", err)
		os.Exit(1)
	}
	if httpAddress != "" {
		http.ServeHTTP(httpAddress, srv)
	}
	// Wait
	<-mainClose
}

func doAutojoin(s *torus.Server) error {
	for {
		ring, err := s.MDS.GetRing()
		if err != nil {
			fmt.Fprintf(os.Stderr, "couldn't get ring: %v\n", err)
			return err
		}
		var newRing torus.Ring
		if r, ok := ring.(torus.RingAdder); ok {
			newRing, err = r.AddPeers(torus.PeerInfoList{
				&models.PeerInfo{
					UUID:        s.MDS.UUID(),
					TotalBlocks: s.Blocks.NumBlocks(),
				},
			})
		} else {
			fmt.Fprintf(os.Stderr, "current ring type cannot support auto-adding\n")
			return err
		}
		if err == torus.ErrExists {
			// We're already a member; we're coming back up.
			return nil
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "couldn't add peer to ring: %v", err)
			return err
		}
		err = s.MDS.SetRing(newRing)
		if err == torus.ErrNonSequentialRing {
			continue
		}
		return err
	}
}
