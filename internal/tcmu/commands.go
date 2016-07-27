package torustcmu

import (
	"github.com/coreos/go-tcmu"
)

func (h *torusHandler) handleSyncCommand(cmd *tcmu.SCSICmd) (tcmu.SCSIResponse, error) {
	clog.Debugf("syncing")
	err := h.file.Sync()
	if err != nil {
		clog.Errorf("sync failed: %v", err.Error())
		return cmd.MediumError(), nil
	}
	return cmd.Ok(), nil
}

func (h *torusHandler) handleReportDeviceID(cmd *tcmu.SCSICmd) (tcmu.SCSIResponse, error) {
	v := h.name
	if len(h.name) > 240 {
		v = v[:240]
	}
	data := make([]byte, 4+len(v)+6)
	data[3] = byte(len(v) + 6)
	copy(data[4:], []byte("torus:"))
	copy(data[10:], []byte(v))
	n, err := cmd.Write(data)
	if err != nil {
		clog.Errorf("reportDeviceID failed: %v", err)
		return cmd.MediumError(), nil
	}
	if n < len(data) {
		clog.Error("reportDeviceID failed: unable to copy enough data")
		return cmd.MediumError(), nil
	}
	return cmd.Ok(), nil
}
