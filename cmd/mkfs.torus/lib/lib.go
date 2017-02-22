package lib

import (
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	blockDevice "github.com/coreos/torus/storage/block_device"
)

const (
	DefaultBlockSize = 1024 * 512
)

func debug(verbose bool, msg string, args ...interface{}) {
	if verbose {
		fmt.Fprintf(os.Stderr, strings.TrimSpace(msg)+"\n", args...)
	}
}

func Mkfs(torusBlockSize uint64, verbose bool, deviceName string) error {
	debug(verbose, "formatting device %s for torus with a cluster block size of %d bytes", deviceName, torusBlockSize)
	deviceFile, err := os.OpenFile(deviceName, os.O_RDWR, 0777|os.ModeDevice)
	if err != nil {
		return err
	}

	deviceSize, err := blockDevice.GetDeviceSize(deviceFile)
	if err != nil {
		return err
	}

	debug(verbose, "this device has a size of %d", deviceSize)

	zeros := make([]byte, blockDevice.BlockSize)

	jumpSize := blockDevice.BlockSize + torusBlockSize
	currOffset := blockDevice.MetadataSize
	for {
		if currOffset >= deviceSize {
			// we've reached the end of the device
			break
		}
		if verbose {
			currentBlock := (currOffset - blockDevice.MetadataSize) / jumpSize
			totalBlocks := (deviceSize - blockDevice.MetadataSize) / jumpSize
			if currOffset == blockDevice.MetadataSize {
				fmt.Fprintf(os.Stderr, "\n")
			}
			fmt.Fprintf(os.Stderr, "\rzero'ing out block headers: %d/%d", currentBlock, totalBlocks)
		}

		_, err = deviceFile.Seek(int64(currOffset), os.SEEK_SET)
		if err != nil {
			return err
		}

		n, err := deviceFile.Write(zeros)
		if err != nil {
			return err
		}
		if uint64(n) != blockDevice.BlockSize {
			return fmt.Errorf("Wrote an unexpected number of bytes? %d", n)
		}

		syscall.Sync()

		currOffset += jumpSize
	}
	if verbose {
		fmt.Fprintf(os.Stderr, "\n")
	}

	debug(verbose, "block headers successfully zero'd, writing metadata...")

	metadata := &blockDevice.Metadata{
		TorusBlockSize:     torusBlockSize,
		FormattedTimestamp: time.Now(),
		UsedBlocks:         0,
	}

	metadataBytes := metadata.Marshal()

	_, err = deviceFile.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}

	n, err := deviceFile.Write(metadataBytes)
	if err != nil {
		return err
	}
	if n != len(metadataBytes) {
		return fmt.Errorf("Wrote an unexpected number of bytes? %d", n)
	}

	debug(verbose, "metadata written, syncing changes to disk...")

	deviceFile.Close()
	syscall.Sync()
	debug(verbose, "device %s formatted successfully", deviceName)

	return nil
}
