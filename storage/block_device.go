package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"

	"golang.org/x/net/context"

	"github.com/coreos/torus"
	blockDevice "github.com/coreos/torus/storage/block_device"
)

func init() {
	torus.RegisterBlockStore("block_device", newBlockDeviceBlockStore)
}

func (d *deviceBlock) readBlockHeader(offset uint64) (blockDevice.BlockHeaders, error) {
	_, err := d.deviceFile.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, blockDevice.BlockSize)
	n, err := d.deviceFile.Read(buf)
	if err != nil {
		return nil, err
	}
	if n != len(buf) {
		return nil, fmt.Errorf("Read an unexpected number of bytes? %d", n)
	}
	hdrs, err := DeserializeBlockHeader(buf)
	if err != nil {
		return nil, err
	}
	return hdrs, nil
}

func SerializeBlockHeader(hdrs blockDevice.BlockHeaders) ([]byte, error) {
	buf := make([]byte, blockDevice.BlockSize)
	err := hdrs.Marshal(buf)
	return buf, err
}

func DeserializeBlockHeader(buf []byte) (blockDevice.BlockHeaders, error) {
	hdrs := blockDevice.NewBlockHeaders()
	err := hdrs.Unmarshal(buf)
	return hdrs, err
}

func (d *deviceBlock) writeBlockHeader(hdrs blockDevice.BlockHeaders, offset uint64) error {
	buf, err := SerializeBlockHeader(hdrs)
	if err != nil {
		return err
	}

	_, err = d.deviceFile.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		return err
	}
	n, err := d.deviceFile.Write(buf)
	if err != nil {
		return err
	}
	if uint64(n) != blockDevice.BlockSize {
		return fmt.Errorf("Wrote an unexpected number of bytes? %d", n)
	}
	return nil
}

type deviceBlock struct {
	deviceFile *os.File

	metadata   *blockDevice.Metadata
	deviceSize uint64

	cfg        torus.Config
	globalMeta torus.GlobalMetadata
}

func newBlockDeviceBlockStore(name string, cfg torus.Config, meta torus.GlobalMetadata) (torus.BlockStore, error) {
	f, err := os.OpenFile(cfg.BlockDevice, os.O_RDWR, 0777|os.ModeDevice)
	if err != nil {
		return nil, err
	}

	d := &deviceBlock{
		deviceFile: f,
		cfg:        cfg,
		globalMeta: meta,
	}

	deviceSize, err := blockDevice.GetDeviceSize(f)
	if err != nil {
		return nil, err
	}

	d.deviceSize = deviceSize

	mdata, err := d.readMetadata()
	if err != nil {
		f.Close()
		return nil, err
	}

	d.metadata = mdata

	if mdata.TorusBlockSize != meta.BlockSize {
		f.Close()
		return nil, fmt.Errorf("device %s has been formatted with block size %d, and the cluster is using block size %d", cfg.BlockDevice, mdata.TorusBlockSize, meta.BlockSize)
	}

	return d, nil
}

func (d *deviceBlock) Kind() string {
	return "block_device"
}

func (d *deviceBlock) Flush() error {
	return d.deviceFile.Sync()
}

func (d *deviceBlock) Close() error {
	return d.deviceFile.Close()
}

func (d *deviceBlock) HasBlock(ctx context.Context, b torus.BlockRef) (bool, error) {
	_, found, err := d.findBlockOffset(b)
	return found, err
}

func (d *deviceBlock) GetBlock(ctx context.Context, b torus.BlockRef) ([]byte, error) {
	offset, found, err := d.findBlockOffset(b)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("block not found")
	}
	_, err = d.deviceFile.Seek(int64(offset+blockDevice.BlockSize), os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, d.metadata.TorusBlockSize)
	n, err := d.deviceFile.Read(buf)
	if err != nil {
		return nil, err
	}
	if uint64(n) != d.metadata.TorusBlockSize {
		return nil, fmt.Errorf("Read an unexpected number of bytes? %d", n)
	}
	return buf, nil
}

func (d *deviceBlock) WriteBlock(ctx context.Context, b torus.BlockRef, data []byte) error {
	if uint64(len(data)) != d.metadata.TorusBlockSize {
		return fmt.Errorf("data buffer does not match block size")
	}
	offset, found, err := d.findBlockOffset(b)
	if found {
		// This block already exists! Let's overwrite the data and leave it at
		// that.
		err = d.writeData(offset+blockDevice.BlockSize, data)
		if err != nil {
			return err
		}
		return nil
	}

	// This block does NOT already exist. Let's create it!
	offset, err = d.findDefaultBlockOffset(b)
	if err != nil {
		return err
	}
	hdrs, err := d.readBlockHeader(offset)
	if err != nil {
		return err
	}
	if !hdrs.IsUsed() {
		// The default block isn't used! Let's use it!
		err = d.writeData(offset+blockDevice.BlockSize, data)
		if err != nil {
			return err
		}
		// And update the headers to say we used it
		err = hdrs.Add(&blockDevice.BlockHeader{
			Volume:   uint64(b.Volume()),
			Inode:    uint64(b.INode),
			Index:    uint64(b.Index),
			Location: 0,
		})
		if err != nil {
			return err
		}
		err = d.writeBlockHeader(hdrs, offset)
		if err != nil {
			return err
		}
		// Done! Let's update the used blocks counter
		err = d.incrementUsedBlocks()
		if err != nil {
			return err
		}
		return nil
	}

	// The default block is used. Let's find an unused block.
	jumpSize := blockDevice.BlockSize + d.metadata.TorusBlockSize
	currOffset := offset + jumpSize
	for {
		if currOffset > d.deviceSize-jumpSize {
			currOffset = blockDevice.MetadataSize
		}
		if currOffset == offset {
			// oh my, it looks like we looped all the way around the device
			return fmt.Errorf("device is full")
		}
		currHdrs, err := d.readBlockHeader(currOffset)
		if err != nil {
			return err
		}
		if !currHdrs.IsUsed() {
			// Found one!
			err = d.writeData(currOffset+blockDevice.BlockSize, data)
			if err != nil {
				return err
			}
			// Update the default headers to point to here
			err = hdrs.Add(&blockDevice.BlockHeader{
				Volume:   uint64(b.Volume()),
				Inode:    uint64(b.INode),
				Index:    uint64(b.Index),
				Location: currOffset,
			})
			if err != nil {
				return err
			}
			err = d.writeBlockHeader(hdrs, offset)
			if err != nil {
				return err
			}
			// And finally update these headers to say we used this
			err = currHdrs.Add(&blockDevice.BlockHeader{
				Volume:   uint64(b.Volume()),
				Inode:    uint64(b.INode),
				Index:    uint64(b.Index),
				Location: 0,
			})
			if err != nil {
				return err
			}
			err = d.writeBlockHeader(currHdrs, currOffset)
			if err != nil {
				return err
			}
			// Done! Let's update the used blocks counter
			err = d.incrementUsedBlocks()
			if err != nil {
				return err
			}
			return nil
		}
		currOffset += jumpSize
	}
}

func (d *deviceBlock) writeData(offset uint64, data []byte) error {
	_, err := d.deviceFile.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		return err
	}
	n, err := d.deviceFile.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("Wrote an unexpected number of bytes? %d", n)
	}
	return nil
}

func (d *deviceBlock) WriteBuf(ctx context.Context, b torus.BlockRef) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

func (d *deviceBlock) DeleteBlock(ctx context.Context, b torus.BlockRef) error {
	// Find the block
	offset, found, err := d.findBlockOffset(b)
	if !found {
		return fmt.Errorf("block doesn't exist")
	}
	// Read its headers
	hdrs, err := d.readBlockHeader(offset)
	if err != nil {
		return err
	}
	// Update the headers to not include this block
	hdrs.Delete(b)
	// Write the updated headers
	err = d.writeBlockHeader(hdrs, offset)
	if err != nil {
		return err
	}
	// Done! Let's update the used blocks counter
	err = d.decrementUsedBlocks()
	if err != nil {
		return err
	}
	return nil
}

func (d *deviceBlock) NumBlocks() uint64 {
	return (d.deviceSize - blockDevice.MetadataSize) / d.metadata.TorusBlockSize
}

func (d *deviceBlock) UsedBlocks() uint64 {
	return d.metadata.UsedBlocks
}

type deviceBlockIterator struct {
	db           *deviceBlock
	deviceSize   uint64
	currOffset   uint64
	currBlockRef torus.BlockRef
	done         bool
	err          error
}

func (d *deviceBlock) BlockIterator() torus.BlockIterator {
	i := &deviceBlockIterator{
		db:           d,
		done:         false,
		currBlockRef: torus.BlockRefFromUint64s(0, 0, 0),
	}
	return i
}

func (i *deviceBlockIterator) Err() error {
	return i.err
}

func (i *deviceBlockIterator) Next() bool {
	if i.currOffset == 0 {
		i.currOffset = blockDevice.MetadataSize
		return i.nextHelper(true)
	}
	return i.nextHelper(false)
}

func (i *deviceBlockIterator) nextHelper(firstRun bool) bool {
	if i.done {
		return false
	}
	jumpSize := blockDevice.BlockSize + i.db.metadata.TorusBlockSize
	if !firstRun {
		i.currOffset += jumpSize
	}
searchLoop:
	for {
		if i.currOffset >= i.db.deviceSize {
			i.done = true
			return false
		}
		hdrs, err := i.db.readBlockHeader(i.currOffset)
		if err != nil {
			i.err = err
			return false
		}
		for _, h := range hdrs {
			if h.Location == 0 && !h.IsNil() {
				i.currBlockRef = torus.BlockRefFromUint64s(h.Volume, h.Inode, h.Index)
				break searchLoop
			}
		}
		i.currOffset += jumpSize
	}
	return true
}

func (i *deviceBlockIterator) BlockRef() torus.BlockRef {
	return i.currBlockRef
}

func (i *deviceBlockIterator) Close() error {
	return nil
}

func (d *deviceBlock) BlockSize() uint64 {
	return d.metadata.TorusBlockSize
}

func (d *deviceBlock) readMetadata() (*blockDevice.Metadata, error) {
	// A single 4k block
	buf := make([]byte, blockDevice.BlockSize)
	_, err := d.deviceFile.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	n, err := d.deviceFile.Read(buf)
	if err != nil {
		return nil, err
	}
	if uint64(n) != blockDevice.BlockSize {
		return nil, fmt.Errorf("Read an unexpected number of bytes? %d", n)
	}

	m := &blockDevice.Metadata{}
	err = m.Unmarshal(buf)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// findBlockOffset finds the offset into the device a block's header is.
// Returns the blocks offset, whether or not the block was found, and any errors
func (d *deviceBlock) findBlockOffset(b torus.BlockRef) (uint64, bool, error) {
	blockOffset, err := d.findDefaultBlockOffset(b)
	if err != nil {
		return 0, false, err
	}

	return d.findRealBlockOffset(b, blockOffset)
}

func (d *deviceBlock) findDefaultBlockOffset(b torus.BlockRef) (uint64, error) {
	numBlockLocations, err := d.numBlockLocations()
	if err != nil {
		return 0, err
	}

	buf := make([]byte, 24)
	b.ToBytesBuf(buf)
	h := sha256.New()
	h.Write(buf)
	hashedBuf := h.Sum(buf)
	hashedNum := binary.BigEndian.Uint64(hashedBuf[len(hashedBuf)-8:])
	blockNum := hashedNum % numBlockLocations

	blockOffset := blockNum*(blockDevice.BlockSize+d.metadata.TorusBlockSize) + blockDevice.MetadataSize

	return blockOffset, nil
}

func (d *deviceBlock) findRealBlockOffset(b torus.BlockRef, currentBlockOffset uint64) (uint64, bool, error) {
	hdrs, err := d.readBlockHeader(currentBlockOffset)
	if err != nil {
		return 0, false, err
	}
	for _, h := range hdrs {
		if h.Volume == uint64(b.Volume()) && h.Inode == uint64(b.INode) && h.Index == uint64(b.Index) {
			if h.Location == 0 {
				return currentBlockOffset, true, nil
			} else {
				return d.findRealBlockOffset(b, h.Location)
			}
		}
	}

	return 0, false, nil
}

func (d *deviceBlock) numBlockLocations() (uint64, error) {
	return (d.deviceSize - blockDevice.MetadataSize) / (blockDevice.BlockSize + d.metadata.TorusBlockSize), nil
}

func (d *deviceBlock) incrementUsedBlocks() error {
	d.metadata.UsedBlocks++
	return d.writeMetadata()
}

func (d *deviceBlock) decrementUsedBlocks() error {
	d.metadata.UsedBlocks--
	return d.writeMetadata()
}

func (d *deviceBlock) writeMetadata() error {
	buf := d.metadata.Marshal()
	_, err := d.deviceFile.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}
	n, err := d.deviceFile.Write(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return fmt.Errorf("Unexpected number of bytes written!! %d", n)
	}
	return nil
}
