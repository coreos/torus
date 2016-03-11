package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/DeanThompson/ginpprof"
	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
)

var daemonCommand = &cobra.Command{
	Use:   "daemon HOST:PORT",
	Short: "control attach/detach and mount/unmount remotely",
	Run:   daemonAction,
}

type Daemon struct {
	router      *gin.Engine
	srv         agro.Server
	promHandler http.Handler
	attached    []*attached
	mounts      []*mount
}

type attached struct {
	dev    string
	volume *models.Volume
	closer chan bool
}

type mount struct {
	dir string
	dev *attached
}

type Command struct {
	MountDev string
	MountDir string
	FSType   string
	Volume   VolumeData
}

type VolumeData struct {
	VolumeName string `json:"volume"`
	Trim       bool   `json:"trim"`
}

type Response struct {
	// Status of the callout. One of "Success" or "Failure".
	Status string
	// Message is the reason for failure.
	Message string `json:",omitempty"`
	// Device assigned by the driver.
	Device string `json:"device,omitempty"`
}

func daemonAction(cmd *cobra.Command, args []string) {
	srv := createServer()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	closer := make(chan bool)
	go func() {
		for _ = range signalChan {
			fmt.Println("\nReceived an interrupt, disconnecting...")
			close(closer)
		}
	}()
	err := NewDaemon(srv).router.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "daemon error: %s", err)
		os.Exit(1)
	}
}

func NewDaemon(srv agro.Server) *Daemon {
	engine := gin.New()
	engine.Use(gin.Recovery())
	d := &Daemon{
		router:      engine,
		srv:         srv,
		promHandler: prometheus.Handler(),
	}
	d.setupRoutes()
	return d
}

func (d *Daemon) setupRoutes() {
	d.router.POST("/attach", d.attach)
	d.router.POST("/detach", d.detach)
	d.router.POST("/mount", d.mount)
	d.router.POST("/unmount", d.unmount)
	d.router.GET("/metrics", d.prometheus)
	ginpprof.Wrapper(d.router)
}

func (d *Daemon) prometheus(c *gin.Context) {
	d.promHandler.ServeHTTP(c.Writer, c.Request)
}

func (d *Daemon) attach(c *gin.Context) {
	cmd, err := d.getCommand(c)
	if err != nil {
		d.onErr(c, err)
		return
	}
	vol, err := d.srv.GetVolume(cmd.Volume.VolumeName)
	if err != nil {
		d.onErr(c, err)
		return
	}
	if vol.Type != models.Volume_BLOCK {
		d.onErr(c, errors.New("can't mount file volumes at this time"))
		return
	}
}
func (d *Daemon) detach(c *gin.Context)  {}
func (d *Daemon) mount(c *gin.Context)   {}
func (d *Daemon) unmount(c *gin.Context) {}

func (d *Daemon) writeResponse(c *gin.Context, resp Response) {
	b, err := json.Marshal(resp)
	if err != nil {
		c.Writer.WriteHeader(http.StatusInternalServerError)
		c.Writer.Write([]byte(err.Error()))
	}
	c.Writer.Write(b)
}

func (d *Daemon) onErr(c *gin.Context, err error) {
	c.Writer.WriteHeader(http.StatusInternalServerError)
	d.writeResponse(c, Response{
		Status:  "Failure",
		Message: err.Error(),
	})
}

func (d *Daemon) getCommand(c *gin.Context) (Command, error) {
	var cmd Command
	dec := json.NewDecoder(c.Request.Body)
	err := dec.Decode(&cmd)
	return cmd, err
}
