package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/coreos/agro"
	"github.com/coreos/agro/internal/nbd"
	_ "github.com/coreos/agro/metadata/etcd"
	_ "github.com/coreos/agro/metadata/temp"
	"github.com/coreos/agro/server"
	_ "github.com/coreos/agro/storage/block"
	"github.com/coreos/pkg/capnslog"
)

var etcdAddress = flag.String("etcd", "127.0.0.1:2378", "Etcd")
var volume = flag.String("vol", "", "Agro block volume")

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "agronbd")

func main() {
	flag.Parse()
	capnslog.SetGlobalLogLevel(capnslog.DEBUG)
	cfg := agro.Config{
		MetadataAddress: *etcdAddress,
		ReadCacheSize:   40 * 1024 * 1024,
	}
	srv, err := server.NewServer(cfg, "etcd", "temp")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	srv.OpenReplication()
	blocksrv, err := srv.Block()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	f, err := blocksrv.OpenBlockFile(*volume)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fi, err := f.Stat()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	stats, _ := srv.Info()
	clog.Trace("create")
	handle := nbd.Create(f, int64(fi.Size()), int64(stats.BlockSize))
	dev, err := handle.Connect()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("*****", dev)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func(n *nbd.NBD) {
		for _ = range signalChan {
			fmt.Println("\nReceived an interrupt, disconnecting...")
			n.Close()
		}
	}(handle)

	err = handle.Serve()
	clog.Debug("closing")
	f.Close()
	if err != nil {
		fmt.Println(err)
	}
}
