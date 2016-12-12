package torus

import (
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/pkg/capnslog"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/coreos/torus/models"
)

var (
	promOpenINodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "torus_server_open_inodes",
		Help: "Number of open inodes reported on last update to mds",
	}, []string{"volume"})
	promOpenFiles = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "torus_server_open_files",
		Help: "Number of open files",
	}, []string{"volume"})
	promFileSyncs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "torus_server_file_syncs",
		Help: "Number of times a file has been synced on this server",
	}, []string{"volume"})
	promFileChangedSyncs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "torus_server_file_changed_syncs",
		Help: "Number of times a file has been synced on this server, and the file has changed underneath it",
	}, []string{"volume"})
	promFileWrittenBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "torus_server_file_written_bytes",
		Help: "Number of bytes written to a file on this server",
	}, []string{"volume"})
	promFileBlockRead = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "torus_server_file_block_read_us",
		Help:    "Histogram of ms taken to read a block through the layers and into the file abstraction",
		Buckets: prometheus.ExponentialBuckets(50.0, 2, 20),
	})
	promFileBlockWrite = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "torus_server_file_block_write_us",
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
	mut      sync.RWMutex
	srv      *Server
	blkSize  int64
	offset   int64
	ReadOnly bool

	// file metadata
	volume   *models.Volume
	inode    *models.INode
	blocks   Blockset
	replaces uint64
	changed  map[string]bool
	cache    fileCache

	writeINodeRef INodeRef
	writeOpen     bool
}

func (f *File) WriteOpen() bool {
	return f.writeOpen
}

func (f *File) Replaces() uint64 {
	return f.replaces
}

func (s *Server) CreateFile(volume *models.Volume, inode *models.INode, blocks Blockset) (*File, error) {
	md := s.MDS.GlobalMetadata()
	clog.Tracef("Creating File For Inode %d:%d", inode.Volume, inode.INode)
	return &File{
		volume:  volume,
		inode:   inode,
		srv:     s,
		blocks:  blocks,
		blkSize: int64(md.BlockSize),
		cache:   newSingleBlockCache(blocks, md.BlockSize),
	}, nil
}

func (f *File) openWrite() error {
	if f.ReadOnly {
		return ErrLocked
	}
	if f.writeOpen {
		return nil
	}
	vid := VolumeID(f.volume.Id)
	newINode, err := f.srv.MDS.CommitINodeIndex(vid)
	if err != nil {
		return err
	}
	f.writeINodeRef = NewINodeRef(VolumeID(vid), newINode)
	if f.inode != nil {
		f.replaces = f.inode.INode
		f.inode.INode = uint64(newINode)
	}
	f.writeOpen = true
	f.cache.newINode(f.writeINodeRef)
	return nil
}

func (f *File) writeToBlock(i, from, to int, data []byte) (int, error) {
	return f.cache.writeToBlock(f.getContext(), i, from, to, data)
}

func (f *File) getContext() context.Context {
	return f.srv.getContext()
}

func (f *File) Write(b []byte) (n int, err error) {
	n, err = f.WriteAt(b, f.offset)
	f.offset += int64(n)
	return
}

func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	f.mut.Lock()
	defer f.mut.Unlock()
	err = f.openWrite()
	if err != nil {
		return 0, err
	}

	if clog.LevelAt(capnslog.TRACE) {
		clog.Trace("begin write: offset ", off, " size ", len(b))
	}
	toWrite := len(b)

	defer func() {
		if off > int64(f.inode.Filesize) {
			clog.Tracef("updating filesize: %d", off)
			f.inode.Filesize = uint64(off)
		}
	}()

	// Write the front matter, which may dangle from a byte offset
	blkIndex := int(off / f.blkSize)

	if f.blocks.Length()+1 < blkIndex {
		if clog.LevelAt(capnslog.DEBUG) {
			clog.Debug("begin write: offset ", off, " size ", len(b))
			clog.Debug("end of file ", f.blocks.Length(), " blkIndex ", blkIndex)
		}
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
		wrote, err := f.writeToBlock(blkIndex, int(blkOff), int(blkOff)+frontlen, b[:frontlen])
		clog.Tracef("head writing block at index %d, inoderef %s", blkIndex, f.writeINodeRef)
		if err != nil {
			return n, err
		} else if wrote != frontlen {
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
		if clog.LevelAt(capnslog.TRACE) {
			clog.Tracef("bulk writing block at index %d, inoderef %s", blkIndex, f.writeINodeRef)
		}
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
	wrote, err := f.writeToBlock(blkIndex, 0, toWrite, b)
	if clog.LevelAt(capnslog.TRACE) {
		clog.Tracef("tail writing block at index %d, inoderef %s", blkIndex, f.writeINodeRef)
	}
	if err != nil {
		promFileWrittenBytes.WithLabelValues(f.volume.Name).Add(float64(n))
		return n, err
	} else if wrote != toWrite {
		promFileWrittenBytes.WithLabelValues(f.volume.Name).Add(float64(n))
		return n, errors.New("Couldn't write all of the last block")
	}
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
		clog.Trace("begin read: offset ", off, " size ", toRead)
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
		blk, err := f.cache.getBlock(f.getContext(), blkIndex)
		if err != nil {
			return n, err
		}
		thisRead := f.blkSize - blkOff
		if int64(toRead-n) < thisRead {
			thisRead = int64(toRead - n)
		}
		count := copy(b[n:], blk[blkOff:blkOff+thisRead])
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
	promOpenFiles.WithLabelValues(f.volume.Name).Dec()
	return nil
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
	err := f.SyncBlocks()
	if err != nil {
		return ZeroINode(), err
	}
	return f.SyncINode(f.getContext())
}

func (f *File) SyncINode(ctx context.Context) (INodeRef, error) {
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
	err = f.srv.INodes.WriteINode(ctx, ref, f.inode)
	if err != nil {
		return ZeroINode(), err
	}
	f.writeOpen = false
	return ref, nil
}

func (f *File) SyncBlocks() error {
	err := f.cache.sync(f.getContext())
	if err != nil {
		clog.Error("sync: couldn't sync block")
		return err
	}
	return f.srv.Blocks.Flush()
}

func (f *File) Size() uint64 {
	return f.inode.Filesize
}
