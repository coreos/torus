package http

import (
	"net/http"
	"strings"

	"github.com/barakmich/agro"
	"github.com/julienschmidt/httprouter"
)

type apiV1 struct {
	srv agro.Server
}

func (a *apiV1) SetupRoutes(h *httprouter.Router) error {
	h.PUT("/api/v1/volume/:volume", a.CreateVolume)
	h.GET("/api/v1/volume", a.GetVolumes)
	return nil
}

func (a *apiV1) CreateVolume(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	vol := ps.ByName("volume")
	err := a.srv.CreateVolume(vol)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
	}
	w.WriteHeader(200)
}

func (a *apiV1) GetVolumes(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	list, err := a.srv.GetVolumes()
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
	}
	w.Write([]byte(strings.Join(list, "\n")))
}
