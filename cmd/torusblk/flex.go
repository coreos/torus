package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/coreos/go-systemd/dbus"
	"github.com/coreos/go-systemd/unit"
	"github.com/coreos/torus"
	"github.com/coreos/torus/internal/nbd"
	godbus "github.com/godbus/dbus"
	"github.com/kardianos/osext"
	"github.com/spf13/cobra"
)

type VolumeData struct {
	VolumeName     string `json:"volume"`
	Trim           bool   `json:"trim"`
	Etcd           string `json:"etcd"`
	FSType         string `json:"kubernetes.io/fsType"`
	ReadWrite      string `json:"kubernetes.io/readwrite"`
	WriteLevel     string `json:"writeLevel"`
	WriteCacheSize string `json:"writeCacheSize"`
}

type Response struct {
	// Status of the callout. One of "Success" or "Failure".
	Status string `json:"status"`
	// Message is the reason for failure.
	Message string `json:"message,omitempty"`
	// Device assigned by the driver.
	Device string `json:"device,omitempty"`
}

var initCommand = &cobra.Command{
	Use:   "init",
	Short: "flex: init",
	Run:   initAction,
}

var attachCommand = &cobra.Command{
	Use:   "attach",
	Short: "flex: attach",
	Run:   attachAction,
}

var mountCommand = &cobra.Command{
	Use:   "mount",
	Short: "flex: mount",
	Run:   mountAction,
}

var unmountCommand = &cobra.Command{
	Use:   "unmount",
	Short: "flex: unmount",
	Run:   unmountAction,
}

var detachCommand = &cobra.Command{
	Use:   "detach",
	Short: "flex: detach",
	Run:   detachAction,
}

var flexprepvolCommand = &cobra.Command{
	Use:   "flexprepvol",
	Short: "flex: prepvol",
	Run:   flexprepvolAction,
}

func initAction(cmd *cobra.Command, args []string) {
	writeResponse(Response{
		Status: "Success",
	})
}

func devToUnitName(dev string) string {
	return "torus-" + unit.UnitNamePathEscape(dev) + ".service"
}

type systemd struct {
	*dbus.Conn
	evChan  <-chan map[string]*dbus.UnitStatus
	errChan <-chan error
}

func connectSystemd() systemd {
	var sysd systemd
	conn, err := dbus.New()
	if err != nil {
		onErr(err)
	}
	err = conn.Subscribe()
	if err != nil {
		onErr(err)
	}

	err = conn.Unsubscribe()
	if err != nil {
		onErr(err)
	}
	sysd.Conn = conn
	sysd.evChan, sysd.errChan = conn.SubscribeUnits(time.Second)
	return sysd
}

func (s systemd) wait(svc string) string {
	for {
		select {
		case changes := <-s.evChan:
			tCh, ok := changes[svc]

			// Just continue until we see our event.
			if !ok {
				continue
			}
			return tCh.ActiveState
		case err := <-s.errChan:
			onErr(err)
		}
	}
}

func parseJSONArg(s string) VolumeData {
	var vol VolumeData
	err := json.Unmarshal([]byte(s), &vol)
	if err != nil {
		onErr(err)
	}
	if vol.VolumeName == "" {
		onErr(errors.New("volume name is missing"))
	}
	if vol.FSType == "" {
		vol.FSType = "ext4"
	}
	if vol.Etcd == "" {
		vol.Etcd = "127.0.0.1:2379"
	}
	return vol
}

//{"kubernetes.io/fsType":"ext4","kubernetes.io/readwrite":"rw","volume":"block1"}
func attachAction(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		onErr(errors.New("unexpected number of arguments"))
	}
	vol := parseJSONArg(args[0])

	dev, err := nbd.FindDevice()
	if err != nil {
		onErr(err)
	}

	sysd := connectSystemd()

	svc := devToUnitName(dev)

	me, err := osext.Executable()
	if err != nil {
		onErr(err)
	}

	cmdList := []string{
		me,
		"-C",
		vol.Etcd,
		"nbd",
		vol.VolumeName,
		dev,
	}
	if vol.WriteLevel != "" {
		_, err := torus.ParseWriteLevel(vol.WriteLevel)
		if err != nil {
			onErr(err)
		}
		cmdList = append(cmdList, []string{"--write-level", vol.WriteLevel}...)
	}
	if vol.WriteCacheSize != "" {
		cmdList = append(cmdList, []string{"--write-cache-size", vol.WriteCacheSize}...)
	}

	ch := make(chan string)

	sysd.ResetFailedUnit(svc)
	_, err = sysd.StartTransientUnit(svc, "fail", []dbus.Property{
		dbus.PropExecStart(cmdList, false),
	}, ch)
	if err != nil {
		onErr(err)
	}
	<-ch
	status := sysd.wait(svc)
	if status == "failed" {
		onErr(errors.New("couldn't attach"))
	} else if status == "active" {
		writeResponse(Response{
			Status: "Success",
			Device: dev,
		})
	} else {
		onErr(errors.New(status))
	}
	os.Exit(0)
}

func mountAction(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		onErr(errors.New("unexpected number of arguments"))
	}

	vol := parseJSONArg(args[2])

	mountdir := args[0]
	mountdev := args[1]
	oneshotsvc := devToUnitName(mountdir)
	// mountsvc := pathToMountName(mountdir)

	me, err := osext.Executable()
	if err != nil {
		onErr(err)
	}

	flags := "noatime"
	if vol.Trim {
		flags = "noatime,discard"
	}

	ch := make(chan string)
	// ch2 := make(chan string)

	sysd := connectSystemd()

	sysd.ResetFailedUnit(oneshotsvc)
	_, err = sysd.StartTransientUnit(oneshotsvc, "fail", []dbus.Property{
		dbus.Property{
			Name:  "Type",
			Value: godbus.MakeVariant("oneshot"),
		},
		dbus.PropExecStart([]string{
			me,
			"flexprepvol",
			mountdev,
			vol.FSType,
		}, false),
	}, ch)
	if err != nil {
		onErr(err)
	}
	s := <-ch
	if s == "failed" {
		onErr(errors.New(s))
	}
	// _, err = sysd.StartTransientUnit(mountsvc, "fail", []dbus.Property{
	// 	dbus.Property{
	// 		Name:  "What",
	// 		Value: godbus.MakeVariant(mountdev),
	// 	},
	// 	dbus.Property{
	// 		Name:  "Where",
	// 		Value: godbus.MakeVariant(mountdir),
	// 	},
	// 	dbus.Property{
	// 		Name:  "Type",
	// 		Value: godbus.MakeVariant(vol.FSType),
	// 	},
	// 	dbus.Property{
	// 		Name:  "Options",
	// 		Value: godbus.MakeVariant(flags),
	// 	},
	// }, ch2)
	// if err != nil {
	// 	onErr(err)
	// }
	// status := sysd.wait(mountsvc)
	// if status == "failed" {
	// 	onErr(errors.New("couldn't attach"))
	// } else if status == "active" {
	// 	writeResponse(Response{
	// 		Status: "Success",
	// 		Device: mountdev,
	// 	})
	// }
	if err := os.MkdirAll(mountdir, os.ModeDir|0555); err != nil {
		onErr(err)
	}

	ex := exec.Command("mount", "-t", vol.FSType, "-o", flags, mountdev, mountdir)
	_, err = ex.CombinedOutput()
	if err != nil {
		onErr(err)
	}
	writeResponse(Response{
		Status: "Success",
		Device: mountdev,
	})
	os.Exit(0)
}

func unmountAction(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		onErr(errors.New("unexpected number of arguments"))
	}
	mountdir := args[0]
	// svc := pathToMountName(mountdir)
	// sysd := connectSystemd()
	// ch := make(chan string)
	// sysd.StopUnit(svc, "fail", ch)
	// <-ch

	_, err := exec.Command("umount", mountdir).Output()
	if err != nil {
		onErr(err)
	}
	writeResponse(Response{
		Status: "Success",
	})
	os.Exit(0)
}

func detachAction(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		onErr(errors.New("unexpected number of arguments"))
	}
	dev := args[0]
	svc := devToUnitName(dev)
	sysd := connectSystemd()
	sysd.KillUnit(svc, 2)
	sysd.ResetFailedUnit(svc)
	writeResponse(Response{
		Status: "Success",
		Device: dev,
	})
	os.Exit(0)
}

func flexprepvolAction(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		onErr(errors.New("unexpected number of arguments"))
	}
	dev := args[0]
	fstype := args[1]
	out, err := exec.Command("blkid", "-p", dev).Output()
	if err != nil {
		// Not formatted
		out, err := exec.Command("mkfs", "-t", fstype, dev).CombinedOutput()
		if err != nil {
			fmt.Println(string(out))
			os.Exit(1)
		}
	} else {
		if !strings.Contains(string(out), fstype) {
			// wrong FS type, this is bad
			fmt.Println("unexpected FS Type")
			os.Exit(1)
		}
	}
	os.Exit(0)
}

func writeResponse(resp Response) {
	b, err := json.Marshal(resp)
	if err != nil {
		fmt.Println([]byte(err.Error()))
		os.Exit(2)
	}
	fmt.Print(string(b))
}

func onErr(err error) {
	writeResponse(Response{
		Status:  "Failure",
		Message: err.Error(),
	})
	os.Exit(1)
}
