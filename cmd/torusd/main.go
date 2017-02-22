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
	"github.com/coreos/torus/internal/flagconfig"
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
	dataDir     string
	blockDevice string
	httpAddress string
	peerAddress string
	sizeStr     string
	host        string
	port        int
	debugInit   bool
	autojoin    bool
	logpkg      string
	cfg         torus.Config

	debug      bool
	version    bool
	completion bool
)

var rootCommand = &cobra.Command{
	Use:    "torusd",
	Short:  "Torus distributed storage",
	Long:   `The torus distributed storage server.`,
	PreRun: configureServer,
	Run:    runServer,
}

func init() {
	rootCommand.PersistentFlags().StringVarP(&blockDevice, "block-device", "", "/dev/thing", "Path to a torus formatted block device")
	rootCommand.PersistentFlags().StringVarP(&dataDir, "data-dir", "", "torus-data", "Path to the data directory")
	rootCommand.PersistentFlags().BoolVarP(&debug, "debug", "", false, "Turn on debug output")
	rootCommand.PersistentFlags().BoolVarP(&debugInit, "debug-init", "", false, "Run a default init for the MDS if one doesn't exist")
	rootCommand.PersistentFlags().StringVarP(&host, "host", "", "", "Host to listen on for HTTP")
	rootCommand.PersistentFlags().IntVarP(&port, "port", "", 4321, "Port to listen on for HTTP")
	rootCommand.PersistentFlags().StringVarP(&peerAddress, "peer-address", "", "", "Address to listen on for intra-cluster data")
	rootCommand.PersistentFlags().StringVarP(&sizeStr, "size", "", "1GiB", "How much disk space to use for this storage node")
	rootCommand.PersistentFlags().StringVarP(&logpkg, "logpkg", "", "", "Specific package logging")
	rootCommand.PersistentFlags().BoolVarP(&autojoin, "auto-join", "", false, "Automatically join the storage pool")
	rootCommand.PersistentFlags().BoolVarP(&version, "version", "", false, "Print version info and exit")
	rootCommand.PersistentFlags().BoolVarP(&completion, "completion", "", false, "Output bash completion code")
	flagconfig.AddConfigFlags(rootCommand.PersistentFlags())
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

	var (
		err  error
		size uint64
	)
	if strings.Contains(sizeStr, "%") {
		percent, err := parsePercentage(sizeStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error parsing size %s: %s\n", sizeStr, err)
			os.Exit(1)
		}
		directory, _ := filepath.Abs(dataDir)
		size = du.NewDiskUsage(directory).Size() * percent / 100
	} else {
		size, err = humanize.ParseBytes(sizeStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error parsing size %s: %s\n", sizeStr, err)
			os.Exit(1)
		}
	}

	cfg = flagconfig.BuildConfigFromFlags()
	cfg.DataDir = dataDir
	cfg.BlockDevice = blockDevice
	cfg.StorageSize = size
}

func parsePercentage(percentString string) (uint64, error) {
	sizePercent := strings.Split(percentString, "%")[0]
	sizeNumber, err := strconv.Atoi(sizePercent)
	if err != nil {
		return 0, err
	}
	if sizeNumber < 1 || sizeNumber > 100 {
		return 0, errors.New(fmt.Sprintf("invalid size %d; must be between 1%% and 100%%", sizeNumber))
	}
	return uint64(sizeNumber), nil
}

func runServer(cmd *cobra.Command, args []string) {
	if completion {
		cmd.Root().GenBashCompletion(os.Stdout)
		os.Exit(0)
	}

	var (
		srv *torus.Server
		err error
	)
	switch {
	case cfg.MetadataAddress == "":
		srv, err = torus.NewServer(cfg, "temp", "mfile")
	case debugInit:
		err = torus.InitMDS("etcd", cfg, torus.GlobalMetadata{
			BlockSize:        512 * 1024,
			DefaultBlockSpec: blockset.MustParseBlockLayerSpec("crc,base"),
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
	case blockDevice != "":
		srv, err = torus.NewServer(cfg, "etcd", "block_device")
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
		if err == torus.ErrNonSequentialRing || err == torus.ErrAgain {
			fmt.Fprintf(os.Stderr, "failed to set ring, try again: %v", err)
			continue
		}
		return err
	}
}
