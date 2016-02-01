package http

import (
	"io"
	"net/http"
	"strings"

	"github.com/DeanThompson/ginpprof"
	"github.com/coreos/agro"
	"github.com/coreos/pkg/capnslog"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "http")

type Server struct {
	router      *gin.Engine
	dfs         agro.Server
	promHandler http.Handler
}

func NewServer(dfs agro.Server) *Server {
	s := &Server{
		router:      gin.Default(),
		dfs:         dfs,
		promHandler: prometheus.Handler(),
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
		v0.DELETE("/volume/:volume/file/:filename", s.deleteFile)
	}
	s.router.GET("/metrics", s.prometheus)
	ginpprof.Wrapper(s.router)
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

func (s *Server) prometheus(c *gin.Context) {
	s.promHandler.ServeHTTP(c.Writer, c.Request)
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
	})
	if err != nil {
		c.Writer.WriteHeader(http.StatusInternalServerError)
		c.Writer.Write([]byte(err.Error()))
		return
	}
	defer f.Close()
	_, err = io.Copy(f, c.Request.Body)
	if err != nil {
		clog.Error(err)
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

func (s *Server) deleteFile(c *gin.Context) {
	vol := c.Params.ByName("volume")
	filename := c.Params.ByName("filename")
	err := s.dfs.Remove(agro.Path{
		Volume: vol,
		Path:   "/" + filename,
	})
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
