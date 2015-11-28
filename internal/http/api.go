package http

import (
	"io"
	"net/http"
	"strings"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
	"github.com/gin-gonic/gin"
)

type Server struct {
	router *gin.Engine
	dfs    agro.Server
}

func NewServer(dfs agro.Server) *Server {
	s := &Server{
		router: gin.Default(),
		dfs:    dfs,
	}
	s.setupRoutes()
	return s
}

func (s *Server) setupRoutes() {
	v0 := s.router.Group("/v0")
	{
		v0.PUT("/volume/:volume", s.createVolume)
		v0.GET("/volume", s.getVolumes)
		v0.PUT("/volume/:volume/file/:filename", s.putFile)
		v0.GET("/volume/:volume/file/:filename", s.getFile)
	}
}

func (s *Server) createVolume(c *gin.Context) {
	vol := c.Params.ByName("volume")
	err := s.dfs.CreateVolume(vol)
	if err != nil {
		c.Writer.WriteHeader(http.StatusInternalServerError)
		c.Writer.Write([]byte(err.Error()))
	}
	c.Writer.WriteHeader(http.StatusCreated)
}

func (s *Server) getVolumes(c *gin.Context) {
	list, err := s.dfs.GetVolumes()
	if err != nil {
		c.Writer.WriteHeader(http.StatusInternalServerError)
		c.Writer.Write([]byte(err.Error()))
	}
	c.Writer.Write([]byte(strings.Join(list, "\n")))
}

func (s *Server) putFile(c *gin.Context) {
	vol := c.Params.ByName("volume")
	filename := c.Params.ByName("filename")
	f, err := s.dfs.Create(agro.Path{
		Volume: vol,
		Path:   "/" + filename,
	}, models.Metadata{})
	if err != nil {
		c.Writer.WriteHeader(http.StatusInternalServerError)
		c.Writer.Write([]byte(err.Error()))
		return
	}
	defer f.Close()
	_, err = io.Copy(f, c.Request.Body)
	if err != nil {
		c.Writer.WriteHeader(http.StatusInternalServerError)
		c.Writer.Write([]byte(err.Error()))
		return
	}
	c.Writer.WriteHeader(http.StatusCreated)
}

func (s *Server) getFile(c *gin.Context) {
	vol := c.Params.ByName("volume")
	filename := c.Params.ByName("filename")
	f, err := s.dfs.Open(agro.Path{
		Volume: vol,
		Path:   "/" + filename,
	})
	if err != nil {
		c.Writer.WriteHeader(http.StatusInternalServerError)
		c.Writer.Write([]byte(err.Error()))
		return
	}
	defer f.Close()
	_, err = io.Copy(c.Writer, f)
	if err != nil {
		c.Writer.WriteHeader(http.StatusInternalServerError)
		c.Writer.Write([]byte(err.Error()))
		return
	}
	c.Writer.WriteHeader(http.StatusOK)
}
func ServeHTTP(addr string, srv agro.Server) error {
	return NewServer(srv).router.Run(addr)
}
