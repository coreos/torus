package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"

	"github.com/DeanThompson/ginpprof"
	"github.com/coreos/agro"
	"github.com/coreos/agro/internal/nbd"
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
	err := NewDaemon(srv, closer).router.Run(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "daemon error: %s", err)
		os.Exit(1)
	}
}

func NewDaemon(srv agro.Server, closer chan bool) *Daemon {
	engine := gin.Default()
	//engine.Use(gin.Recovery())
	d := &Daemon{
		router:      engine,
		srv:         srv,
		promHandler: prometheus.Handler(),
	}
	go closeAll(d, closer)
	d.setupRoutes()
	return d
}

func closeAll(d *Daemon, closer chan bool) {
	<-closer
	for _, x := range d.mounts {
		_, err := exec.Command("umount", x.dir).Output()
		if err != nil {
			clog.Error(err)
		}
	}

	for _, x := range d.attached {
		close(x.closer)
	}
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
		clog.Error("parse")
		d.onErr(c, err)
		return
	}
	vol, err := d.srv.GetVolume(cmd.Volume.VolumeName)
	if err != nil {
		clog.Error("vol")
		d.onErr(c, err)
		return
	}
	if vol.Type != models.Volume_BLOCK {
		d.onErr(c, errors.New("can't mount file volumes at this time"))
		return
	}

	dev, err := nbd.FindDevice()
	if err != nil {
		d.onErr(c, err)
		clog.Error("find")
		return
	}
	closer := make(chan bool)
	a := &attached{
		dev:    dev,
		closer: closer,
		volume: vol,
	}
	go connectNBD(d.srv, vol.Name, dev, closer)
	d.attached = append(d.attached, a)
	d.writeResponse(c, Response{
		Status: "Success",
		Device: dev,
	})
}

func (d *Daemon) detach(c *gin.Context) {
	cmd, err := d.getCommand(c)
	if err != nil {
		d.onErr(c, err)
		return
	}
	for i, x := range d.attached {
		if x.dev == cmd.MountDev {
			close(x.closer)
			d.attached = append(d.attached[:i], d.attached[i+1:]...)
			d.writeResponse(c, Response{
				Status: "Success",
				Device: x.dev,
			})
			return
		}
	}
	d.onErr(c, errors.New("Device not found"))
}
func (d *Daemon) mount(c *gin.Context) {
	cmd, err := d.getCommand(c)
	if err != nil {
		d.onErr(c, err)
		return
	}
	// blkid -p dev
	found := false
	var attach *attached
	for _, x := range d.attached {
		if x.dev == cmd.MountDev {
			found = true
			attach = x
			break
		}
	}
	if !found {
		d.onErr(c, errors.New("device not attached"))
		return
	}
	fstype := cmd.FSType
	if fstype == "" {
		fstype = "ext4"
	}
	clog.Debug(fstype)
	out, err := exec.Command("blkid", "-p", cmd.MountDev).Output()
	if err != nil {
		// Not formatted
		clog.Debug("blkid err")
		out, err := exec.Command("mkfs", "-t", fstype, cmd.MountDev).Output()
		clog.Info("mkfs ", string(out))
		clog.Info("err ", err)
	} else {
		if !strings.Contains(string(out), fstype) {
			// wrong FS type, this is bad
			d.onErr(c, errors.New("unexpected FS type"))
			return
		}
	}
	flags := "noatime"
	if cmd.Volume.Trim {
		flags = "noatime,discard"
	}
	ex := exec.Command("mount", "-t", fstype, "-o", flags, cmd.MountDev, cmd.MountDir)
	fmt.Println(ex.Env)
	out, err = ex.CombinedOutput()
	if err != nil {
		clog.Debug("mount err", string(out))
		d.onErr(c, err)
		return
	}
	d.mounts = append(d.mounts, &mount{
		dir: cmd.MountDir,
		dev: attach,
	})
	d.writeResponse(c, Response{
		Status: "Success",
		Device: attach.dev,
	})
}

func (d *Daemon) unmount(c *gin.Context) {
	cmd, err := d.getCommand(c)
	if err != nil {
		d.onErr(c, err)
		return
	}
	for i, x := range d.mounts {
		if x.dir == cmd.MountDir {
			_, err := exec.Command("umount", cmd.MountDir).Output()
			if err != nil {
				d.onErr(c, err)
				return
			}
			d.mounts = append(d.mounts[:i], d.mounts[i+1:]...)
			d.writeResponse(c, Response{
				Status: "Success",
				Device: x.dev.dev,
			})
			return
		}
	}
	d.onErr(c, errors.New("Device not found"))
}

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
	buf := bufio.NewReader(c.Request.Body)
	str, _ := buf.ReadString('\n')
	clog.Debug(str)
	//dec := json.NewDecoder(.Request.Body)
	//err := dec.Decode(&cmd)
	err := json.Unmarshal([]byte(str), &cmd)
	return cmd, err
}
