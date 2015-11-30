package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/coreos/pkg/capnslog"

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

var debug = flag.Bool("debug", false, "Turn on debug output")
var mkfs = flag.Bool("debug-mkfs", false, "Run mkfs for the given ")
var trace = flag.Bool("trace", false, "Turn on debug output")
var etcd = flag.String("etcd", "", "Address for talking to etcd")
var port = flag.String("port", "4321", "Port to listen on for HTTP")
var datadir = flag.String("datadir", "/tmp/agro", "Path to the data directory")

func main() {
	var err error
	flag.Parse()

	capnslog.SetGlobalLogLevel(capnslog.INFO)
	if *debug {
		capnslog.SetGlobalLogLevel(capnslog.DEBUG)
	}
	if *trace {
		capnslog.SetGlobalLogLevel(capnslog.TRACE)
	}

	cfg := agro.Config{
		DataDir:         *datadir,
		StorageSize:     1 * 1024 * 1024 * 1024,
		MetadataAddress: *etcd,
	}

	var srv agro.Server
	if *etcd == "" {
		srv, err = server.NewServer(cfg, "temp", "bolt", "mfile")
	} else {
		if *mkfs {
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
		}
		srv, err = server.NewServer(cfg, "etcd", "bolt", "mfile")
		if err != nil {
			fmt.Printf("Couldn't start: %s\n", err)
			os.Exit(1)
		}
	}
	//	srv := server.NewMemoryServer()
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

	http.ServeHTTP("127.0.0.1:"+*port, srv)
}
