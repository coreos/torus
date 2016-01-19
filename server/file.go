package server

import (
	"errors"
	"io"
	"sync"

	"golang.org/x/net/context"

	"github.com/coreos/pkg/capnslog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tgruben/roaring"

	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/models"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "server")

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
	path   agro.Path
	inode  *models.INode
	flags  int
	offset int64
	blocks agro.Blockset

	// during write
	initialINodes *roaring.RoaringBitmap
	writeINodeRef agro.INodeRef
	writeOpen     bool

	// half-finished blocks
	openIdx  int
	openData []byte
}

func (s *server) newFile(path agro.Path, inode *models.INode) (agro.File, error) {
	bs, err := blockset.UnmarshalFromProto(inode.GetBlocks(), s.blocks)
	if err != nil {
		return nil, err
	}
	md, err := s.mds.GlobalMetadata()
	if err != nil {
		return nil, err
	}

	set := bs.GetLiveINodes()
	s.incRef(path.Volume, set)
	bm, _ := s.getBitmap(path.Volume)
	err = s.mds.ClaimVolumeINodes(path.Volume, bm)
	if err != nil {
		s.decRef(path.Volume, set)
		return nil, err
	}
	promOpenINodes.WithLabelValues(path.Volume).Set(float64(bm.GetCardinality()))

	clog.Debugf("Open file %s at inode %d:%d with block length %d and size %d", path, inode.Volume, inode.INode, bs.Length(), inode.Filesize)
	f := &file{
		path:          path,
		inode:         inode,
		srv:           s,
		offset:        0,
		blocks:        bs,
		initialINodes: set,
		blkSize:       int64(md.BlockSize),
	}
	promOpenFiles.WithLabelValues(path.Volume).Inc()
	return f, nil
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
	f.writeINodeRef = agro.INodeRef{
		Volume: vid,
		INode:  newINode,
	}
	if f.inode != nil {
		f.inode.Replaces = f.inode.INode
		f.inode.INode = uint64(newINode)
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
	if f.openData == nil {
		panic("server: file data not open")
	}
	if (to - from) != len(data) {
		panic("server: different write lengths?")
	}
	return copy(f.openData[from:to], data)
}

func (f *file) syncBlock() error {
	if f.openData == nil {
		return nil
	}
	err := f.blocks.PutBlock(context.TODO(), f.writeINodeRef, f.openIdx, f.openData)
	f.openIdx = -1
	f.openData = nil
	return err
}

func (f *file) WriteAt(b []byte, off int64) (n int, err error) {
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
			f.inode.Filesize = uint64(off)
		}
	}()

	// Write the front matter, which may dangle from a byte offset
	blkIndex := int(off / f.blkSize)

	if f.blocks.Length() < blkIndex && blkIndex != f.openIdx+1 {
		// TODO(barakmich) Support truncate in the block abstraction, fill/return 0s
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
		err = f.blocks.PutBlock(context.TODO(), f.writeINodeRef, blkIndex, b[:f.blkSize])
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
	clog.Tracef("begin read of size %d", toRead)
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
		clog.Tracef("getting block index %d", blkIndex)
		blk, err := f.blocks.GetBlock(context.TODO(), blkIndex)
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
	promOpenFiles.WithLabelValues(f.path.Volume).Dec()
	return err
}

func (f *file) Sync() error {
	return f.sync(false)
}

func (f *file) sync(closing bool) error {
	// Here there be dragons.
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
	err = f.srv.inodes.WriteINode(context.TODO(), f.writeINodeRef, f.inode)
	if err != nil {
		clog.Error("sync: couldn't write inode")
		return err
	}

	// TODO(barakmich): Starting with SetFileINode, there's currently a problem in
	// that, if the following bookkeeping is interrupted (eg, the machine we're on
	// dies in the middle of the rest of this function) there could be an unknown
	// garbage state. Ideally, it needs to be "the mother of all transactions",
	// which is totally possible, but I'm holding off on doing that right now
	// until I know better how these work together. Once it becomes clear, the
	// optimization/bugfix can be made.

	replaced, err := f.srv.mds.SetFileINode(f.path, f.writeINodeRef)
	if err != nil {
		clog.Error("sync: couldn't set file inode")
		return err
	}
	newLive := f.blocks.GetLiveINodes()
	switch replaced {
	case agro.INodeID(0):
		// Easy. We're creating or replacing a deleted file.
		// TODO(barakmich): Correct behavior depending on O_CREAT
		dead := roaring.NewRoaringBitmap()
		f.srv.mds.ModifyDeadMap(f.path.Volume, newLive, dead)
	case agro.INodeID(f.inode.Replaces):
		// Easy. We're replacing the inode we opened. This is the happy case.
		newLive := f.blocks.GetLiveINodes()
		dead := roaring.AndNot(f.initialINodes, newLive)
		f.srv.mds.ModifyDeadMap(f.path.Volume, newLive, dead)
	default:
		// Dammit. Somebody changed the file underneath us. We can write a smarter
		// merge function -- O_APPEND for example, doing the right thing, by keeping
		// some state in the file and actually appending it.
		//
		// Today, however, we're going to go with the technically correct but perhaps
		// suboptimal one: last write wins the good news is, we're that last write.
		promFileChangedSyncs.WithLabelValues(f.path.Volume).Inc()
		var newINode *models.INode
		for {
			newINode, err = f.srv.inodes.GetINode(context.TODO(), agro.INodeRef{
				Volume: f.writeINodeRef.Volume,
				INode:  replaced,
			})
			if err == nil {
				break
			}
			// We can't go back, and we can't go forward, which is why this is a
			// critical section of doom. See the above TODO. In a full transaction,
			// we can safely bail.
			clog.Error("sync: can't get inode we replaced")
		}
		bs, err := blockset.UnmarshalFromProto(newINode.Blocks, nil)
		if err != nil {
			// If it's corrupt we're in another world of hurt. But this one we can't fix.
			// Again, safer in transaction.
			panic("sync: couldn't unmarshal blockset")
		}
		oldLive := bs.GetLiveINodes()
		dead := roaring.AndNot(oldLive, newLive)
		f.srv.mds.ModifyDeadMap(f.path.Volume, newLive, dead)
	}
	// Critical section over.
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
	promOpenINodes.WithLabelValues(f.path.Volume).Set(float64(bm.GetCardinality()))
	err := f.srv.mds.ClaimVolumeINodes(f.path.Volume, bm)
	if err != nil {
		clog.Error("file: TODO: Can't re-claim")
	}
}
