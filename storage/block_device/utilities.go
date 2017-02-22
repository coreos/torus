package block_device

// #include <fcntl.h>
// #include <linux/fs.h>
import "C"

import (
	"os"
	"syscall"
	"unsafe"
)

func GetDeviceSize(deviceFile *os.File) (uint64, error) {
	var numBlocks int64
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(deviceFile.Fd()), uintptr(C.BLKGETSIZE), uintptr(unsafe.Pointer(&numBlocks)))
	if errno != 0 {
		return 0, errno
	}
	numBytes := numBlocks * 512
	return uint64(numBytes), nil
}
