package http

import (
	"fmt"
	"net/http"

	"github.com/barakmich/agro"
	"github.com/julienschmidt/httprouter"
)

func ServeHTTP(host string, port int, srv agro.Server) error {
	h, err := NewHandler(srv)
	if err != nil {
		return err
	}
	return http.ListenAndServe(fmt.Sprintf("%s:%d", host, port), h)
}

func NewHandler(srv agro.Server) (*httprouter.Router, error) {
	h := httprouter.New()
	api := &apiV1{
		srv: srv,
	}
	err := api.SetupRoutes(h)
	return h, err
}
