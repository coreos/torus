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
	// The SCSI spec only allows lengths representable in one byte (byte 3). We
	// also need some overhead; reporting the device came from Torus Therefore, we
	// need to truncate the length of the name to something less than 255. Let's
	// truncate it to 240.
	if len(h.name) > 240 {
		v = v[:240]
	}
	name := []byte("torus:" + v)
	data := make([]byte, 4+len(name))
	data[3] = byte(len(name))
	copy(data[4:], name)
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
		// Realloc; the buffer can be reused. See io.Copy() in the
		// stdlib for precedent.
		cmd.Buf = make([]byte, length)
	}
	n, err := cmd.Read(cmd.Buf[:int(length)])
	if err != nil {
		clog.Errorf("write/read failed: error: %v", err)
		return cmd.MediumError(), nil
	}
	if n < length {
		clog.Error("write/read failed: unable to copy enough")
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
		// Write10/Write12 CDB 1 is defined as follows:
		// Bits --->
		// |   7   |   6   |   5   |   4   |   3   |   2   |   1   |   0   |
		// |       WRPROTECT       |  DPO  |  FUA  | resvd | FUANV | resvd |
		// So 0x08 represents the Force Unit Access bit being set, which, by
		// spec, requires a sync and not to return until it's been written.
		if cdbinfo&0x08 != 0 {
			// FUA is set
			err = h.file.Sync()
			if err != nil {
				clog.Errorf("sync failed: %v", err)
				return cmd.MediumError(), nil
			}
		}
	}
	return cmd.Ok(), nil
}
