package lib

import (
	"fmt"
	"os"
	"strings"

	blockDevice "github.com/coreos/torus/storage/block_device"
)

func stderr(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, strings.TrimSpace(msg)+"\n", args...)
}

func debug(verbose bool, msg string, args ...interface{}) {
	if verbose {
		stderr(msg, args...)
	}
}

func Fsck(verbose bool, deviceName string, torusBlockSize uint64) error {
	debug(verbose, "checking device %s for torus with a cluster block size of %d bytes", deviceName, torusBlockSize)
	deviceFile, err := os.OpenFile(deviceName, os.O_RDWR, 0777|os.ModeDevice)
	if err != nil {
		return err
	}
	defer deviceFile.Close()

	deviceSize, err := blockDevice.GetDeviceSize(deviceFile)
	if err != nil {
		return err
	}

	debug(verbose, "this device has a size of %d", deviceSize)

	buf := make([]byte, blockDevice.BlockSize)
	_, err = deviceFile.Read(buf)
	if err != nil {
		return err
	}
	metadata := blockDevice.Metadata{}
	err = metadata.Unmarshal(buf)
	if err != nil {
		return err
	}
	if metadata.TorusBlockSize != torusBlockSize {
		return fmt.Errorf("device has a torus block size of %d, whereas we're checking for %d", metadata.TorusBlockSize, torusBlockSize)
	}
	debug(verbose, "torus was formatted on %s", metadata.FormattedTimestamp.String())
	stderr("there should be %d blocks in use", metadata.UsedBlocks)

	jumpSize := blockDevice.BlockSize + torusBlockSize
	currOffset := blockDevice.MetadataSize
	index := 0
	var usedBlocks uint64 = 0
	for {
		index++
		if currOffset >= deviceSize {
			// we've reached the end of the device
			break
		}

		_, err = deviceFile.Seek(int64(currOffset), os.SEEK_SET)
		if err != nil {
			return err
		}

		n, err := deviceFile.Read(buf)
		if err != nil {
			return err
		}
		if uint64(n) != blockDevice.BlockSize {
			return fmt.Errorf("Read an unexpected number of bytes? %d", n)
		}

		hdrs := blockDevice.NewBlockHeaders()
		err = hdrs.Unmarshal(buf)
		if err != nil {
			return err
		}
		for _, h := range hdrs {
			if h.Location >= deviceSize {
				stderr("Found a block header claiming that block %d,%d,%d is at location %d, which is off the disk!", h.Volume, h.Inode, h.Index, h.Location)
				stderr("Printing all of the headers for this block:")
				for _, h := range hdrs {
					fmt.Printf(" - volume:   %d\n", h.Volume)
					fmt.Printf(" - inode:    %d\n", h.Inode)
					fmt.Printf(" - index:    %d\n", h.Index)
					fmt.Printf(" - location: %d\n", h.Location)
					fmt.Printf("------------\n")
				}
				stderr("This is at block #%d, offset %d", index, currOffset)
				return fmt.Errorf("This block got corrupted somehow")
			}
			if !h.IsNil() && h.Location == 0 {
				usedBlocks++
			}
		}

		currOffset += jumpSize
	}
	deviceFile.Close()
	if usedBlocks != metadata.UsedBlocks {
		return fmt.Errorf("there were supposed to be %d blocks in use, but I found %d in use", metadata.UsedBlocks, usedBlocks)
	}
	stderr("device %s passes fsck", deviceName)

	return nil
}
