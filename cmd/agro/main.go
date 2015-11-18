package main

import (
	"github.com/barakmich/agro/internal/http"
	"github.com/barakmich/agro/server"

	_ "github.com/barakmich/agro/metadata/temp"
)

func main() {
	srv := server.NewMemoryServer()
	http.ServeHTTP("127.0.0.1:4321", srv)
}
