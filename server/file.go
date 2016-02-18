package server

import (
	"errors"
	"io"
	"os"
	"sync"

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
)

func init() {
	prometheus.MustRegister(promOpenINodes)
	prometheus.MustRegister(promOpenFiles)
	prometheus.MustRegister(promFileSyncs)
	prometheus.MustRegister(promFileChangedSyncs)
	prometheus.MustRegister(promFileWrittenBytes)
}

type file struct {
	// globals
	mut     sync.RWMutex
	srv     *server
	blkSize int64

	// file metadata
	path     agro.Path
	inode    *models.INode
	flags    int
	offset   int64
	blocks   agro.Blockset
	replaces uint64
	changed  map[string]bool

	// during write
	initialINodes *roaring.Bitmap
	writeINodeRef agro.INodeRef
	writeOpen     bool
	writeLevel    agro.WriteLevel

	// half-finished blocks
	openIdx   int
	openData  []byte
	openWrote bool

	readOnly  bool
	writeOnly bool
}

func (f *file) Write(b []byte) (n int, err error) {
	n, err = f.WriteAt(b, f.offset)
	f.offset += int64(n)
	return
}

func (f *file) openWrite() error {
	if f.writeOpen {
		return nil
	}
	f.srv.writeableLock.RLock()
	defer f.srv.writeableLock.RUnlock()
	vid, err := f.srv.mds.GetVolumeID(f.path.Volume)
	if err != nil {
		return err
	}
	newINode, err := f.srv.mds.CommitINodeIndex(f.path.Volume)
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
	return nil
}

func (f *file) openBlock(i int) error {
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
	d, err := f.blocks.GetBlock(context.TODO(), i)
	if err != nil {
		return err
	}
	f.openData = d
	f.openIdx = i
	return nil
}

func (f *file) writeToBlock(from, to int, data []byte) int {
	f.openWrote = true
	if f.openData == nil {
		panic("server: file data not open")
	}
	if (to - from) != len(data) {
		panic("server: different write lengths?")
	}
	return copy(f.openData[from:to], data)
}

func (f *file) syncBlock() error {
	if f.openData == nil || !f.openWrote {
		return nil
	}
	err := f.blocks.PutBlock(f.getContext(), f.writeINodeRef, f.openIdx, f.openData)
	f.openIdx = -1
	f.openData = nil
	f.openWrote = false
	return err
}

func (f *file) getContext() context.Context {
	return context.WithValue(context.TODO(), ctxWriteLevel, f.writeLevel)
}

func (f *file) WriteAt(b []byte, off int64) (n int, err error) {
	f.mut.Lock()
	defer f.mut.Unlock()
	clog.Trace("begin write: offset ", off, " size ", len(b))
	if f.writeOnly {
		f.Truncate(off)
	}
	toWrite := len(b)
	err = f.openWrite()
	if err != nil {
		return 0, err
	}

	defer func() {
		if off > int64(f.inode.Filesize) {
			f.inode.Filesize = uint64(off)
		}
	}()

	// Write the front matter, which may dangle from a byte offset
	blkIndex := int(off / f.blkSize)

	if f.blocks.Length() < blkIndex && blkIndex != f.openIdx+1 {
		clog.Debug("begin write: offset ", off, " size ", len(b))
		clog.Debug("end of file ", f.blocks.Length(), " blkIndex ", blkIndex)
		promFileWrittenBytes.WithLabelValues(f.path.Volume).Add(float64(n))
		return n, errors.New("Can't write past the end of a file")
	}

	blkOff := off - int64(int(f.blkSize)*blkIndex)
	if blkOff != 0 {
		frontlen := int(f.blkSize - blkOff)
		if frontlen > toWrite {
			frontlen = toWrite
		}
		err := f.openBlock(blkIndex)
		if err != nil {
			promFileWrittenBytes.WithLabelValues(f.path.Volume).Add(float64(n))
			return n, err
		}
		wrote := f.writeToBlock(int(blkOff), int(blkOff)+frontlen, b[:frontlen])
		clog.Tracef("head writing block at index %d, inoderef %s", blkIndex, f.writeINodeRef)
		if wrote != frontlen {
			promFileWrittenBytes.WithLabelValues(f.path.Volume).Add(float64(n))
			return n, errors.New("Couldn't write all of the first block at the offset")
		}
		b = b[frontlen:]
		n += wrote
		off += int64(wrote)
	}

	toWrite = len(b)
	if toWrite == 0 {
		// We're done
		promFileWrittenBytes.WithLabelValues(f.path.Volume).Add(float64(n))
		return n, nil
	}

	// Bulk Write! We'd rather be here.
	if off%f.blkSize != 0 {
		panic("Offset not equal to a block boundary")
	}

	for toWrite >= int(f.blkSize) {
		blkIndex := int(off / f.blkSize)
		clog.Tracef("bulk writing block at index %d, inoderef %s", blkIndex, f.writeINodeRef)
		err = f.blocks.PutBlock(f.getContext(), f.writeINodeRef, blkIndex, b[:f.blkSize])
		if err != nil {
			promFileWrittenBytes.WithLabelValues(f.path.Volume).Add(float64(n))
			return n, err
		}
		b = b[f.blkSize:]
		n += int(f.blkSize)
		off += int64(f.blkSize)
		toWrite = len(b)
	}

	if toWrite == 0 {
		// We're done
		promFileWrittenBytes.WithLabelValues(f.path.Volume).Add(float64(n))
		return n, nil
	}

	// Trailing matter. This sucks too.
	if off%f.blkSize != 0 {
		panic("Offset not equal to a block boundary after bulk")
	}
	blkIndex = int(off / f.blkSize)
	err = f.openBlock(blkIndex)
	if err != nil {
		promFileWrittenBytes.WithLabelValues(f.path.Volume).Add(float64(n))
		return n, err
	}
	wrote := f.writeToBlock(0, toWrite, b)
	clog.Tracef("tail writing block at index %d, inoderef %s", blkIndex, f.writeINodeRef)
	if wrote != toWrite {
		promFileWrittenBytes.WithLabelValues(f.path.Volume).Add(float64(n))
		return n, errors.New("Couldn't write all of the last block")
	}
	b = b[wrote:]
	n += wrote
	off += int64(wrote)
	promFileWrittenBytes.WithLabelValues(f.path.Volume).Add(float64(n))
	return n, nil
}

func (f *file) Read(b []byte) (n int, err error) {
	n, err = f.ReadAt(b, f.offset)
	f.offset += int64(n)
	return
}

func (f *file) ReadAt(b []byte, off int64) (n int, ferr error) {
	f.mut.RLock()
	defer f.mut.RUnlock()
	toRead := len(b)
	if clog.LevelAt(capnslog.TRACE) {
		clog.Tracef("begin read of size %d", toRead)
	}
	n = 0
	if int64(toRead)+off > int64(f.inode.Filesize) {
		toRead = int(int64(f.inode.Filesize) - off)
		ferr = io.EOF
		clog.Tracef("read is longer than file")
	}
	for toRead > n {
		blkIndex := int(off / f.blkSize)
		if f.blocks.Length() <= blkIndex {
			// TODO(barakmich) Support truncate in the block abstraction, fill/return 0s
			return n, io.EOF
		}
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
	err := f.sync(true)
	if err != nil {
		clog.Error(err)
	}
	f.srv.removeOpenFile(f)
	promOpenFiles.WithLabelValues(f.path.Volume).Dec()
	return err
}

func (f *file) Sync() error {
	return f.sync(false)
}

func (f *file) sync(closing bool) error {
	// Here there be dragons.
	clog.Debugf("Syncing file: %s", f.path)
	if !f.writeOpen {
		f.updateHeldINodes(closing)
		return nil
	}
	promFileSyncs.WithLabelValues(f.path.Volume).Inc()
	err := f.syncBlock()
	if err != nil {
		clog.Error("sync: couldn't sync block")
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
			f.path,
			func(inode *models.INode, vol agro.VolumeID) (*models.INode, agro.INodeRef, error) {
				if inode == nil {
					// We're unilaterally overwriting a file. If that was the intent, go ahead.
					if f.replaces == 0 {
						// Replace away.
						return f.inode, f.writeINodeRef, nil
					}
					// Otherwise, nothing to do here.
					return nil, agro.NewINodeRef(vol, agro.INodeID(0)), writingToDeleted
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
		promFileChangedSyncs.WithLabelValues(f.path.Volume).Inc()
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
		f.updateHeldINodes(false)
	}

	err = f.srv.mds.SetFileEntry(f.path, &models.FileEntry{
		Chain: f.inode.Chain,
	})

	newLive := f.blocks.GetLiveINodes()
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
		dead = roaring.AndNot(bs.GetLiveINodes(), newLive)
	}
	f.srv.mds.ModifyDeadMap(f.path.Volume, newLive, dead)

	// Critical section over.
	f.changed = make(map[string]bool)
	f.updateHeldINodes(closing)
	// SHANTIH.
	f.writeOpen = false
	return nil
}

func (f *file) updateHeldINodes(closing bool) {
	f.srv.decRef(f.path.Volume, f.initialINodes)
	if !closing {
		f.initialINodes = f.blocks.GetLiveINodes()
		f.srv.incRef(f.path.Volume, f.initialINodes)
	}
	bm, _ := f.srv.getBitmap(f.path.Volume)
	card := uint64(0)
	if bm != nil {
		card = bm.GetCardinality()
	}
	promOpenINodes.WithLabelValues(f.path.Volume).Set(float64(card))
	err := f.srv.mds.ClaimVolumeINodes(f.path.Volume, bm)
	if err != nil {
		clog.Error("file: TODO: Can't re-claim")
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
