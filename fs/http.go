package fs

// func (s *Server) setupFSRoutes() {
// 	v0 := s.router.Group("/v0")
// 	{
// 		v0.PUT("/volume/:volume", s.createVolume)
// 		v0.GET("/volume", s.getVolumes)
// 		v0.PUT("/volume/:volume/file/:filename", s.putFile)
// 		v0.GET("/volume/:volume/file/:filename", s.getFile)
// 		v0.DELETE("/volume/:volume/file/:filename", s.deleteFile)
// 		v0.GET("/dumpmetadata", s.dumpMDS)
// 	}
// }

// func (s *Server) createVolume(c *gin.Context) {
// 	vol := c.Params.ByName("volume")
// 	err := s.dfs.CreateFSVolume(vol)
// 	if err != nil {
// 		c.Writer.WriteHeader(http.StatusInternalServerError)
// 		c.Writer.Write([]byte(err.Error()))
// 	}
// 	c.Writer.WriteHeader(http.StatusCreated)
// }

// func (s *Server) dumpMDS(c *gin.Context) {
// 	err := s.dfs.Debug(c.Writer)
// 	if err != nil {
// 		c.Writer.WriteHeader(http.StatusInternalServerError)
// 		c.Writer.Write([]byte(err.Error()))
// 	}
// }

// func (s *Server) getVolumes(c *gin.Context) {
// 	vols, err := s.dfs.GetVolumes()
// 	if err != nil {
// 		c.Writer.WriteHeader(http.StatusInternalServerError)
// 		c.Writer.Write([]byte(err.Error()))
// 	}
// 	list := make([]string, len(vols))
// 	for i, x := range vols {
// 		list[i] = x.Name
// 	}
// 	c.Writer.Write([]byte(strings.Join(list, "\n")))
// }

// func (s *Server) putFile(c *gin.Context) {
// 	vol := c.Params.ByName("volume")
// 	filename := c.Params.ByName("filename")
// 	f, err := s.dfs.Create(agro.Path{
// 		Volume: vol,
// 		Path:   "/" + filename,
// 	})
// 	if err != nil {
// 		c.Writer.WriteHeader(http.StatusInternalServerError)
// 		c.Writer.Write([]byte(err.Error()))
// 		return
// 	}
// 	defer f.Close()
// 	_, err = io.Copy(f, c.Request.Body)
// 	if err != nil {
// 		clog.Error(err)
// 		c.Writer.WriteHeader(http.StatusInternalServerError)
// 		c.Writer.Write([]byte(err.Error()))
// 		return
// 	}
// 	c.Writer.WriteHeader(http.StatusCreated)
// }

// func (s *Server) getFile(c *gin.Context) {
// 	vol := c.Params.ByName("volume")
// 	filename := c.Params.ByName("filename")
// 	f, err := s.dfs.Open(agro.Path{
// 		Volume: vol,
// 		Path:   "/" + filename,
// 	})
// 	if err != nil {
// 		c.Writer.WriteHeader(http.StatusInternalServerError)
// 		c.Writer.Write([]byte(err.Error()))
// 		return
// 	}
// 	defer f.Close()
// 	stat, _ := s.dfs.Info()
// 	bs := stat.BlockSize
// 	buf := make([]byte, bs)
// 	_, err = io.CopyBuffer(c.Writer, f, buf)
// 	if err != nil {
// 		c.Writer.WriteHeader(http.StatusInternalServerError)
// 		c.Writer.Write([]byte(err.Error()))
// 		return
// 	}
// 	c.Writer.WriteHeader(http.StatusOK)
// }

// func (s *Server) deleteFile(c *gin.Context) {
// 	vol := c.Params.ByName("volume")
// 	filename := c.Params.ByName("filename")
// 	err := s.dfs.Remove(agro.Path{
// 		Volume: vol,
// 		Path:   "/" + filename,
// 	})
// 	if err != nil {
// 		c.Writer.WriteHeader(http.StatusInternalServerError)
// 		c.Writer.Write([]byte(err.Error()))
// 		return
// 	}
// 	c.Writer.WriteHeader(http.StatusOK)
// }
