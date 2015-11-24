package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/coreos/pkg/capnslog"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/internal/http"
	"github.com/barakmich/agro/server"
)

var debug = flag.Bool("debug", false, "Turn on debug output")

func main() {
	var err error
	flag.Parse()
	capnslog.SetGlobalLogLevel(capnslog.INFO)
	if *debug {
		capnslog.SetGlobalLogLevel(capnslog.DEBUG)
		//	capnslog.MustRepoLogger("github.com/barakmich/agro").SetLogLevel(map[string]capnslog.LogLevel{"block": capnslog.DEBUG})
	}
	cfg := agro.Config{
		DataDir:     "/tmp/agro",
		StorageSize: 200 * 1024 * 1024,
	}
	srv, err := server.NewPersistentServer(cfg)
	//	srv := server.NewMemoryServer()
	if err != nil {
		fmt.Printf("Couldn't start: %s\n", err)
		os.Exit(1)
	}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func() {
		for _ = range signalChan {
			fmt.Println("\nReceived an interrupt, stopping services...")
			srv.Close()
			os.Exit(0)
		}
	}()

	http.ServeHTTP("127.0.0.1:4321", srv)
}
