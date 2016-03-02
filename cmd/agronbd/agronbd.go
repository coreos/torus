package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/coreos/agro"
	_ "github.com/coreos/agro/metadata/etcd"
	_ "github.com/coreos/agro/metadata/temp"
	"github.com/coreos/agro/server"
	_ "github.com/coreos/agro/storage/block"
	"github.com/coreos/pkg/capnslog"
	"github.com/dustin/go-humanize"
	"github.com/frostschutz/go-nbd"
)

var etcdAddress = flag.String("etcd", "127.0.0.1:2378", "Etcd")
var volume = flag.String("vol", "", "Agro volume to file")
var file = flag.String("file", "", "Agro path to file")
var size = flag.String("size", "30MiB", "Size of image")

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "agronbd")

func main() {
	flag.Parse()
	imgsize, err := humanize.ParseBytes(*size)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing size: %s\n", err)
		os.Exit(1)
	}
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
	clog.Trace("open rep")
	srv.OpenReplication()
	path := agro.Path{
		Volume: *volume,
		Path:   filepath.Clean(filepath.Join("/", *file)),
	}
	clog.Trace("lstat")
	fi, err := srv.Lstat(path)
	var f agro.File
	if err == os.ErrNotExist {
		f, err = srv.Create(path)
		f.Truncate(int64(imgsize))
	} else if err != nil {
		panic(err)
	} else {
		f, err = srv.Open(path)
		imgsize = uint64(fi.Size())
	}
	if err != nil {
		panic(err)
	}
	clog.Trace("sync")
	f.Sync()
	fmt.Println("Size:", imgsize)
	go func(f agro.File) {
		for {
			time.Sleep(5 * time.Second)
			f.Sync()
		}
	}(f)
	clog.Trace("create")
	handle := nbd.Create(f, int64(imgsize))
	dev, err := handle.Connect()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("*****", dev)
	err = handle.Serve()
	f.Close()
	fmt.Println(err)
}
