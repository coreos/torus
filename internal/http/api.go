package http

import (
	"net/http"

	"github.com/DeanThompson/ginpprof"
	"github.com/coreos/agro"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

type Server struct {
	router      *gin.Engine
	dfs         *agro.Server
	promHandler http.Handler
}

func NewServer(dfs *agro.Server) *Server {
	engine := gin.New()
	engine.Use(gin.Recovery())
	s := &Server{
		router:      engine,
		dfs:         dfs,
		promHandler: prometheus.Handler(),
	}
	s.setupRoutes()
	return s
}

func (s *Server) setupRoutes() {
	s.router.GET("/metrics", s.prometheus)
	ginpprof.Wrapper(s.router)
}

func (s *Server) prometheus(c *gin.Context) {
	s.promHandler.ServeHTTP(c.Writer, c.Request)
}

func ServeHTTP(addr string, srv *agro.Server) error {
	return NewServer(srv).router.Run(addr)
}

func (s *Server) Run(addr string) error {
	return s.router.Run(addr)
}
