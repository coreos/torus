package torustcmu

import (
	"github.com/coreos/go-tcmu"
	"github.com/coreos/go-tcmu/scsi"
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

func (h *torusHandler) handleWrite(cmd *tcmu.SCSICmd) (tcmu.SCSIResponse, error) {
	offset := cmd.LBA() * uint64(cmd.Device().Sizes().BlockSize)
	length := int(cmd.XferLen() * uint32(cmd.Device().Sizes().BlockSize))
	if cmd.Buf == nil {
		cmd.Buf = make([]byte, length)
	}
	if len(cmd.Buf) < int(length) {
		//realloc
		cmd.Buf = make([]byte, length)
	}
	n, err := cmd.Read(cmd.Buf[:int(length)])
	if n < length {
		clog.Error("write/read failed: unable to copy enough")
		return cmd.MediumError(), nil
	}
	if err != nil {
		clog.Errorf("write/read failed: error: %v", err)
		return cmd.MediumError(), nil
	}
	n, err = h.file.WriteAt(cmd.Buf[:length], int64(offset))
	if n < length {
		clog.Error("write/write failed: unable to copy enough")
		return cmd.MediumError(), nil
	}
	if err != nil {
		clog.Errorf("write/write failed: error: %v", err)
		return cmd.MediumError(), nil
	}
	if cmd.Command() != scsi.Write6 {
		cdbinfo := cmd.GetCDB(1)
		if cdbinfo&0xc != 0 {
			// FUA is set
			err := h.file.Sync()
			if err != nil {
				clog.Errorf("sync failed: %v", err)
				return cmd.MediumError(), nil
			}
		}
	}
	return cmd.Ok(), nil
}
