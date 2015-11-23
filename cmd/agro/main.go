package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/barakmich/agro/internal/http"
	"github.com/barakmich/agro/server"

	_ "github.com/barakmich/agro/metadata/temp"
)

func main() {
	srv := server.NewMemoryServer()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func() {
		for _ = range signalChan {
			fmt.Println("\nReceived an interrupt, stopping services...")
			srv.Close()
			signal.Reset(os.Interrupt)
			os.Exit(0)
		}
	}()

	http.ServeHTTP("127.0.0.1:4321", srv)
}
