package server

import (
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/pkg/capnslog"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/models"
)

var (
	clog             = capnslog.NewPackageLogger("github.com/coreos/agro", "server")
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

type fileHandle struct {
	// globals
	mut     sync.RWMutex
	srv     *server
	blkSize int64

	// file metadata
	volume   string
	inode    *models.INode
	blocks   agro.Blockset
	replaces uint64
	changed  map[string]bool

	// during write
	initialINodes *roaring.Bitmap
	writeINodeRef agro.INodeRef
	writeOpen     bool

	// half-finished blocks
	openIdx   int
	openData  []byte
	openWrote bool
}

type file struct {
	*fileHandle
	flags  int
	offset int64
	path   agro.Path

	readOnly  bool
	writeOnly bool
}

func (f *file) Write(b []byte) (n int, err error) {
	n, err = f.WriteAt(b, f.offset)
	f.offset += int64(n)
	return
}

func (f *fileHandle) openWrite() error {
	if f.writeOpen {
		return nil
	}
	f.srv.writeableLock.RLock()
	defer f.srv.writeableLock.RUnlock()
	vid, err := f.srv.mds.GetVolumeID(f.volume)
	if err != nil {
		return err
	}
	newINode, err := f.srv.mds.CommitINodeIndex(f.volume)
	if err != nil {
		if err == agro.ErrAgain {
			return f.openWrite()
		}
		return err
	}
	f.writeINodeRef = agro.NewINodeRef(vid, newINode)
	if f.inode != nil {
		f.replaces = f.inode.INode
		f.inode.INode = uint64(newINode)
		if f.inode.Chain == 0 {
			f.inode.Chain = uint64(newINode)
		}
	}
	f.writeOpen = true
	f.updateHeldINodes(false)
	bm := roaring.NewBitmap()
	bm.Add(uint32(newINode))
	// Kill the open inode; we'll reopen it if we use it.
	f.srv.mds.ModifyDeadMap(vid, roaring.NewBitmap(), bm)
	return nil
}

func (f *fileHandle) openBlock(i int) error {
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

func (f *fileHandle) writeToBlock(from, to int, data []byte) int {
	f.openWrote = true
	if f.openData == nil {
		panic("server: file data not open")
	}
	if (to - from) != len(data) {
		panic("server: different write lengths?")
	}
	return copy(f.openData[from:to], data)
}

func (f *fileHandle) syncBlock() error {
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

func (f *fileHandle) getContext() context.Context {
	return f.srv.getContext()
}

func (f *file) WriteAt(b []byte, off int64) (n int, err error) {
	if f.writeOnly {
		f.Truncate(off)
	}
	return f.fileHandle.WriteAt(b, off)
}

func (f *fileHandle) WriteAt(b []byte, off int64) (n int, err error) {
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
		promFileWrittenBytes.WithLabelValues(f.volume).Add(float64(n))
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
			promFileWrittenBytes.WithLabelValues(f.volume).Add(float64(n))
			return n, err
		}
		wrote := f.writeToBlock(int(blkOff), int(blkOff)+frontlen, b[:frontlen])
		clog.Tracef("head writing block at index %d, inoderef %s", blkIndex, f.writeINodeRef)
		if wrote != frontlen {
			promFileWrittenBytes.WithLabelValues(f.volume).Add(float64(n))
			return n, errors.New("Couldn't write all of the first block at the offset")
		}
		b = b[frontlen:]
		n += wrote
		off += int64(wrote)
	}

	toWrite = len(b)
	if toWrite == 0 {
		// We're done
		promFileWrittenBytes.WithLabelValues(f.volume).Add(float64(n))
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
			promFileWrittenBytes.WithLabelValues(f.volume).Add(float64(n))
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
		promFileWrittenBytes.WithLabelValues(f.volume).Add(float64(n))
		return n, nil
	}

	// Trailing matter. This sucks too.
	if off%f.blkSize != 0 {
		panic("Offset not equal to a block boundary after bulk")
	}
	blkIndex = int(off / f.blkSize)
	err = f.openBlock(blkIndex)
	if err != nil {
		promFileWrittenBytes.WithLabelValues(f.volume).Add(float64(n))
		return n, err
	}
	wrote := f.writeToBlock(0, toWrite, b)
	clog.Tracef("tail writing block at index %d, inoderef %s", blkIndex, f.writeINodeRef)
	if wrote != toWrite {
		promFileWrittenBytes.WithLabelValues(f.volume).Add(float64(n))
		return n, errors.New("Couldn't write all of the last block")
	}
	b = b[wrote:]
	n += wrote
	off += int64(wrote)
	promFileWrittenBytes.WithLabelValues(f.volume).Add(float64(n))
	return n, nil
}

func (f *file) Read(b []byte) (n int, err error) {
	n, err = f.ReadAt(b, f.offset)
	f.offset += int64(n)
	return
}

func (f *fileHandle) ReadAt(b []byte, off int64) (n int, ferr error) {
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
		panic("Read more than n bytes?")
	}
	return n, ferr
}

func (f *file) Close() error {
	if f == nil {
		return agro.ErrInvalid
	}
	if f.fileHandle == nil {
		return nil
	}
	c := f.inode.Chain
	err := f.Sync()
	if err != nil {
		clog.Error(err)
	}
	f.srv.removeOpenFile(c)
	promOpenFiles.WithLabelValues(f.volume).Dec()
	f.fileHandle = nil
	return err
}

func (f *file) Sync() error {
	f.mut.Lock()
	defer f.mut.Unlock()
	// Here there be dragons.
	if !f.writeOpen {
		f.updateHeldINodes(false)
		return nil
	}
	clog.Debugf("Syncing file: %v", f.inode.Filenames)
	clog.Tracef("inode: %s", f.inode)
	clog.Tracef("replaces: %x, ref: %s", f.replaces, f.writeINodeRef)

	promFileSyncs.WithLabelValues(f.volume).Inc()
	err := f.syncBlock()
	if err != nil {
		clog.Error("sync: couldn't sync block")
		return err
	}
	err = f.srv.blocks.Flush()
	if err != nil {
		return err
	}
	blkdata, err := blockset.MarshalToProto(f.blocks)
	if err != nil {
		clog.Error("sync: couldn't marshal proto")
		return err
	}
	f.inode.Blocks = blkdata

	// Here we begin the critical transaction section

	var replaced agro.INodeRef
	for {
		_, replaced, err = f.srv.updateINodeChain(
			f.getContext(),
			f.path,
			func(inode *models.INode, vol agro.VolumeID) (*models.INode, agro.INodeRef, error) {
				if inode == nil {
					// We're unilaterally overwriting a file, or starting a new chain. If that was the intent, go ahead.
					if f.replaces == 0 {
						// Replace away.
						return f.inode, f.writeINodeRef, nil
					}
					// Update the chain
					f.inode.Chain = f.inode.INode
					return f.inode, f.writeINodeRef, nil
				}
				if inode.Chain != f.inode.Chain {
					// We're starting a new chain, go ahead and replace
					return f.inode, f.writeINodeRef, nil
				}
				switch f.replaces {
				case 0:
					// We're writing a completely new file on this chain.
					return f.inode, f.writeINodeRef, nil
				case inode.INode:
					// We're replacing exactly what we expected to replace. Go for it.
					return f.inode, f.writeINodeRef, nil
				default:
					// Dammit. Somebody changed the file underneath us.
					// Abort transaction, we'll figure out what to do.
					return nil, agro.NewINodeRef(vol, agro.INodeID(inode.INode)), aborter
				}
			})
		if err == nil {
			break
		}
		if err != aborter {
			return err
		}
		// We can write a smarter merge function -- O_APPEND for example, doing the
		// right thing, by keeping some state in the file and actually appending it.
		// Today, it's Last Write Wins.
		promFileChangedSyncs.WithLabelValues(f.volume).Inc()
		oldINode := f.inode
		f.inode, err = f.srv.inodes.GetINode(context.TODO(), replaced)
		if err != nil {
			return err
		}
		f.replaces = f.inode.INode
		f.inode.INode = oldINode.INode
		f.inode.Blocks = oldINode.Blocks
		f.inode.Filesize = oldINode.Filesize

		for k, _ := range f.changed {
			switch k {
			case "mode":
				f.inode.Permissions.Mode = oldINode.Permissions.Mode
			}
		}
		bs, err := blockset.UnmarshalFromProto(f.inode.Blocks, nil)
		if err != nil {
			// If it's corrupt we're in another world of hurt. But this one we can't fix.
			// Again, safer in transaction.
			panic("sync: couldn't unmarshal blockset")
		}
		f.initialINodes = bs.GetLiveINodes()
		f.initialINodes.Add(uint32(f.inode.INode))
		f.updateHeldINodes(false)
		clog.Debugf("retrying critical transaction section")
	}

	err = f.srv.mds.SetFileEntry(f.path, &models.FileEntry{
		Chain: f.inode.Chain,
	})

	newLive := f.getLiveINodes()
	var dead *roaring.Bitmap
	// Cleanup.

	// TODO(barakmich): Correct behavior depending on O_CREAT
	dead = roaring.AndNot(f.initialINodes, newLive)
	if replaced.INode != 0 && f.replaces == 0 {
		deadinode, err := f.srv.inodes.GetINode(context.TODO(), replaced)
		if err != nil {
			return err
		}
		bs, err := blockset.UnmarshalFromProto(deadinode.Blocks, nil)
		if err != nil {
			// If it's corrupt we're in another world of hurt. But this one we can't fix.
			// Again, safer in transaction.
			panic("sync: couldn't unmarshal blockset")
		}
		dead.Or(bs.GetLiveINodes())
		dead.Add(uint32(replaced.INode))
		dead.AndNot(newLive)
	}
	f.srv.mds.ModifyDeadMap(f.writeINodeRef.Volume(), newLive, dead)

	// Critical section over.
	f.changed = make(map[string]bool)
	f.writeOpen = false
	f.updateHeldINodes(false)
	// SHANTIH.
	return nil
}

func (f *fileHandle) getLiveINodes() *roaring.Bitmap {
	bm := f.blocks.GetLiveINodes()
	bm.Add(uint32(f.inode.INode))
	return bm
}

func (f *fileHandle) updateHeldINodes(closing bool) {
	vid, _ := f.srv.mds.GetVolumeID(f.volume)
	f.srv.decRef(f.volume, f.initialINodes)
	if !closing {
		f.initialINodes = f.getLiveINodes()
		f.srv.incRef(f.volume, f.initialINodes)
	}
	bm, _ := f.srv.getBitmap(f.volume)
	card := uint64(0)
	if bm != nil {
		card = bm.GetCardinality()
	}
	promOpenINodes.WithLabelValues(f.volume).Set(float64(card))
	mlog.Tracef("updating claim %s %s", f.volume, bm)
	err := f.srv.mds.ClaimVolumeINodes(vid, bm)
	if err != nil {
		mlog.Error("file: TODO: Can't re-claim")
	}
}

func (f *file) Stat() (os.FileInfo, error) {
	return FileInfo{
		INode: f.inode,
		Path:  f.path,
		Ref:   agro.NewINodeRef(agro.VolumeID(f.inode.Volume), agro.INodeID(f.inode.INode)),
	}, nil
}

func (f *file) Truncate(size int64) error {
	if f.readOnly {
		return os.ErrPermission
	}
	return f.fileHandle.Truncate(size)
}

func (f *fileHandle) Truncate(size int64) error {
	err := f.openWrite()
	if err != nil {
		return err
	}
	nBlocks := (size / f.blkSize)
	if size%f.blkSize != 0 {
		nBlocks++
	}
	clog.Tracef("truncate to %d %d", size, nBlocks)
	f.blocks.Truncate(int(nBlocks))
	f.inode.Filesize = uint64(size)
	return nil
}
