package agro

import (
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/pkg/capnslog"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/coreos/agro/models"
)

var (
	aborter          = errors.New("abort update")
	writingToDeleted = errors.New("writing to deleted file")
)

var (
	promOpenINodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "agro_server_open_inodes",
		Help: "Number of open inodes reported on last update to mds",
	}, []string{"volume"})
	promOpenFiles = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "agro_server_open_files",
		Help: "Number of open files",
	}, []string{"volume"})
	promFileSyncs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_server_file_syncs",
		Help: "Number of times a file has been synced on this server",
	}, []string{"volume"})
	promFileChangedSyncs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_server_file_changed_syncs",
		Help: "Number of times a file has been synced on this server, and the file has changed underneath it",
	}, []string{"volume"})
	promFileWrittenBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "agro_server_file_written_bytes",
		Help: "Number of bytes written to a file on this server",
	}, []string{"volume"})
	promFileBlockRead = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "agro_server_file_block_read_us",
		Help:    "Histogram of ms taken to read a block through the layers and into the file abstraction",
		Buckets: prometheus.ExponentialBuckets(50.0, 2, 20),
	})
	promFileBlockWrite = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "agro_server_file_block_write_us",
		Help:    "Histogram of ms taken to write a block through the layers and into the file abstraction",
		Buckets: prometheus.ExponentialBuckets(50.0, 2, 20),
	})
)

func init() {
	prometheus.MustRegister(promOpenINodes)
	prometheus.MustRegister(promOpenFiles)
	prometheus.MustRegister(promFileSyncs)
	prometheus.MustRegister(promFileChangedSyncs)
	prometheus.MustRegister(promFileWrittenBytes)
	prometheus.MustRegister(promFileBlockRead)
	prometheus.MustRegister(promFileBlockWrite)
}

type File struct {
	// globals
	mut     sync.RWMutex
	srv     *Server
	blkSize int64
	offset  int64

	// file metadata
	volume   *models.Volume
	inode    *models.INode
	blocks   Blockset
	replaces uint64
	changed  map[string]bool

	// during write
	writeINodeRef INodeRef
	writeOpen     bool

	// half-finished blocks
	openIdx   int
	openData  []byte
	openWrote bool
}

func (f *File) WriteOpen() bool {
	return f.writeOpen
}

func (f *File) Replaces() uint64 {
	return f.replaces
}

func (s *Server) CreateFile(volume *models.Volume, inode *models.INode, blocks Blockset) (*File, error) {
	md, err := s.MDS.GlobalMetadata()
	if err != nil {
		return nil, err
	}
	return &File{
		volume:  volume,
		inode:   inode,
		srv:     s,
		blocks:  blocks,
		blkSize: int64(md.BlockSize),
	}, nil
}

func (f *File) openWrite() error {
	if f.writeOpen {
		return nil
	}
	f.srv.writeableLock.RLock()
	defer f.srv.writeableLock.RUnlock()
	vid := VolumeID(f.volume.Id)
	newINode, err := f.srv.MDS.CommitINodeIndex(vid)
	if err != nil {
		if err == ErrAgain {
			return f.openWrite()
		}
		return err
	}
	f.writeINodeRef = NewINodeRef(VolumeID(vid), newINode)
	if f.inode != nil {
		f.replaces = f.inode.INode
		f.inode.INode = uint64(newINode)
		if f.inode.Chain == 0 {
			f.inode.Chain = uint64(newINode)
		}
	}
	f.writeOpen = true
	return nil
}

func (f *File) openBlock(i int) error {
	if f.openIdx == i && f.openData != nil {
		return nil
	}
	if f.openData != nil {
		err := f.syncBlock()
		if err != nil {
			return err
		}
	}
	if f.blocks.Length() == i {
		f.openData = make([]byte, f.blkSize)
		f.openIdx = i
		return nil
	}
	start := time.Now()
	d, err := f.blocks.GetBlock(f.getContext(), i)
	if err != nil {
		return err
	}
	delta := time.Now().Sub(start)
	promFileBlockRead.Observe(float64(delta.Nanoseconds()) / 1000)
	f.openData = d
	f.openIdx = i
	return nil
}

func (f *File) writeToBlock(from, to int, data []byte) int {
	f.openWrote = true
	if f.openData == nil {
		panic("server: file data not open")
	}
	if (to - from) != len(data) {
		panic("server: different write lengths?")
	}
	return copy(f.openData[from:to], data)
}

func (f *File) syncBlock() error {
	if f.openData == nil || !f.openWrote {
		return nil
	}
	start := time.Now()
	err := f.blocks.PutBlock(f.getContext(), f.writeINodeRef, f.openIdx, f.openData)
	delta := time.Now().Sub(start)
	promFileBlockWrite.Observe(float64(delta.Nanoseconds()) / 1000)
	f.openIdx = -1
	f.openData = nil
	f.openWrote = false
	return err
}

func (f *File) getContext() context.Context {
	return f.srv.getContext()
}

func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	f.mut.Lock()
	defer f.mut.Unlock()
	clog.Trace("begin write: offset ", off, " size ", len(b))
	toWrite := len(b)
	err = f.openWrite()
	if err != nil {
		return 0, err
	}

	defer func() {
		if off > int64(f.inode.Filesize) {
			clog.Tracef("updating filesize: %d", off)
			f.inode.Filesize = uint64(off)
		}
	}()

	// Write the front matter, which may dangle from a byte offset
	blkIndex := int(off / f.blkSize)

	if f.blocks.Length() < blkIndex && blkIndex != f.openIdx+1 {
		clog.Debug("begin write: offset ", off, " size ", len(b))
		clog.Debug("end of file ", f.blocks.Length(), " blkIndex ", blkIndex)
		promFileWrittenBytes.WithLabelValues(f.volume.Name).Add(float64(n))
		err := f.Truncate(off)
		if err != nil {
			return n, err
		}
		//return n, errors.New("Can't write past the end of a file")
	}

	blkOff := off - int64(int(f.blkSize)*blkIndex)
	if blkOff != 0 {
		frontlen := int(f.blkSize - blkOff)
		if frontlen > toWrite {
			frontlen = toWrite
		}
		err := f.openBlock(blkIndex)
		if err != nil {
			promFileWrittenBytes.WithLabelValues(f.volume.Name).Add(float64(n))
			return n, err
		}
		wrote := f.writeToBlock(int(blkOff), int(blkOff)+frontlen, b[:frontlen])
		clog.Tracef("head writing block at index %d, inoderef %s", blkIndex, f.writeINodeRef)
		if wrote != frontlen {
			promFileWrittenBytes.WithLabelValues(f.volume.Name).Add(float64(n))
			return n, errors.New("Couldn't write all of the first block at the offset")
		}
		b = b[frontlen:]
		n += wrote
		off += int64(wrote)
	}

	toWrite = len(b)
	if toWrite == 0 {
		// We're done
		promFileWrittenBytes.WithLabelValues(f.volume.Name).Add(float64(n))
		return n, nil
	}

	// Bulk Write! We'd rather be here.
	if off%f.blkSize != 0 {
		panic("Offset not equal to a block boundary")
	}

	for toWrite >= int(f.blkSize) {
		blkIndex := int(off / f.blkSize)
		clog.Tracef("bulk writing block at index %d, inoderef %s", blkIndex, f.writeINodeRef)
		start := time.Now()
		err = f.blocks.PutBlock(f.getContext(), f.writeINodeRef, blkIndex, b[:f.blkSize])
		if err != nil {
			promFileWrittenBytes.WithLabelValues(f.volume.Name).Add(float64(n))
			return n, err
		}
		delta := time.Now().Sub(start)
		promFileBlockWrite.Observe(float64(delta.Nanoseconds()) / 1000)
		b = b[f.blkSize:]
		n += int(f.blkSize)
		off += int64(f.blkSize)
		toWrite = len(b)
	}

	if toWrite == 0 {
		// We're done
		promFileWrittenBytes.WithLabelValues(f.volume.Name).Add(float64(n))
		return n, nil
	}

	// Trailing matter. This sucks too.
	if off%f.blkSize != 0 {
		panic("Offset not equal to a block boundary after bulk")
	}
	blkIndex = int(off / f.blkSize)
	err = f.openBlock(blkIndex)
	if err != nil {
		promFileWrittenBytes.WithLabelValues(f.volume.Name).Add(float64(n))
		return n, err
	}
	wrote := f.writeToBlock(0, toWrite, b)
	clog.Tracef("tail writing block at index %d, inoderef %s", blkIndex, f.writeINodeRef)
	if wrote != toWrite {
		promFileWrittenBytes.WithLabelValues(f.volume.Name).Add(float64(n))
		return n, errors.New("Couldn't write all of the last block")
	}
	b = b[wrote:]
	n += wrote
	off += int64(wrote)
	promFileWrittenBytes.WithLabelValues(f.volume.Name).Add(float64(n))
	return n, nil
}

func (f *File) Read(b []byte) (n int, err error) {
	n, err = f.ReadAt(b, f.offset)
	f.offset += int64(n)
	return
}

func (f *File) ReadAt(b []byte, off int64) (n int, ferr error) {
	f.mut.RLock()
	defer f.mut.RUnlock()
	toRead := len(b)
	if clog.LevelAt(capnslog.TRACE) {
		clog.Tracef("begin read @ %x of size %d", off, toRead)
	}
	n = 0
	if int64(toRead)+off > int64(f.inode.Filesize) {
		toRead = int(int64(f.inode.Filesize) - off)
		ferr = io.EOF
		clog.Tracef("read is longer than file")
	}
	for toRead > n {
		blkIndex := int(off / f.blkSize)
		blkOff := off - int64(int(f.blkSize)*blkIndex)
		if clog.LevelAt(capnslog.TRACE) {
			clog.Tracef("getting block index %d", blkIndex)
		}
		err := f.openBlock(blkIndex)
		if err != nil {
			return n, err
		}
		thisRead := f.blkSize - blkOff
		if int64(toRead-n) < thisRead {
			thisRead = int64(toRead - n)
		}
		count := copy(b[n:], f.openData[blkOff:blkOff+thisRead])
		n += count
		off += int64(count)
	}
	if toRead != n {
		//panic("Read more than n bytes?")
	}
	return n, ferr
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	// TODO(mischief): validate offset
	switch whence {
	case os.SEEK_SET:
		f.offset = offset
	case os.SEEK_CUR:
		f.offset += offset
	case os.SEEK_END:
		//f.offset = int64(f.inode.Filesize) - offset
		fallthrough
	default:
		return 0, errors.New("invalid whence")
	}

	return offset, nil
}

func (f *File) Close() error {
	if f == nil {
		return ErrInvalid
	}
	var err error
	promOpenFiles.WithLabelValues(f.volume.Name).Dec()
	return err
}

func (f *File) Truncate(size int64) error {
	err := f.openWrite()
	if err != nil {
		return err
	}
	nBlocks := (size / f.blkSize)
	if size%f.blkSize != 0 {
		nBlocks++
	}
	clog.Tracef("truncate to %d %d", size, nBlocks)
	f.blocks.Truncate(int(nBlocks), uint64(f.blkSize))
	f.inode.Filesize = uint64(size)
	return nil
}

// Trim zeroes data in the middle of a file.
func (f *File) Trim(offset, length int64) error {
	clog.Debugf("trimming %d %d", offset, length)
	err := f.openWrite()
	if err != nil {
		return err
	}
	// find the block edges
	blkFrom := offset / f.blkSize
	if offset%f.blkSize != 0 {
		blkFrom += 1
	}
	blkTo := (offset + length) / f.blkSize
	return f.blocks.Trim(int(blkFrom), int(blkTo))
}

func (f *File) SyncAllWrites() (INodeRef, error) {
	err := f.syncBlock()
	if err != nil {
		clog.Error("sync: couldn't sync block")
		return ZeroINode(), err
	}
	err = f.srv.Blocks.Flush()
	if err != nil {
		return ZeroINode(), err
	}
	ref := f.writeINodeRef
	blkdata, err := MarshalBlocksetToProto(f.blocks)
	if err != nil {
		clog.Error("sync: couldn't marshal proto")
		return ZeroINode(), err
	}
	f.inode.Blocks = blkdata
	if f.inode.Volume != f.volume.Id {
		panic("mismatched volume and inode volume")
	}
	err = f.srv.INodes.WriteINode(f.getContext(), ref, f.inode)
	if err != nil {
		return ZeroINode(), err
	}
	f.writeOpen = false
	return ref, nil
}
