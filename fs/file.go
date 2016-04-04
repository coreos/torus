package fs

import (
	"os"

	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/models"
	"github.com/tgruben/roaring"
)

func (f *file) Stat() (os.FileInfo, error) {
	return FileInfo{
		INode: f.inode,
		Path:  f.path,
		Ref:   agro.NewINodeRef(agro.VolumeID(f.inode.Volume), agro.INodeID(f.inode.INode)),
	}, nil
}

type file struct {
	*agro.File
	flags         int
	path          agro.Path
	initialINodes *roaring.Bitmap

	readOnly  bool
	writeOnly bool
}

func (f *file) Write(b []byte) (n int, err error) {
	n, err = f.WriteAt(b, f.offset)
	f.offset += int64(n)
	return
}

func (f *file) WriteAt(b []byte, off int64) (n int, err error) {
	if f.writeOnly {
		f.Truncate(off)
	}
	// TODO(barakmich): Track open inodes.
	//if f.volume.Type == models.Volume_FILE {
	//f.updateHeldINodes(false)
	//bm := roaring.NewBitmap()
	//bm.Add(uint32(newINode))
	//// Kill the open inode; we'll reopen it if we use it.
	//f.srv.fsMDS().ModifyDeadMap(vid, roaring.NewBitmap(), bm)
	//}
	return f.File.WriteAt(b, off)
}

//func (* file) Close() {
//c := f.inode.Chain
//err = f.Sync()
//if err != nil {
//clog.Error(err)
//}
//f.srv.removeOpenFile(c)
//}

func (f *file) fileSync(mds agro.FSMetadataService) error {
	// Here there be dragons.
	if !f.writeOpen {
		f.updateHeldINodes(false)
		return nil
	}
	clog.Debugf("Syncing file: %v", f.inode.Filenames)
	clog.Tracef("inode: %s", f.inode)
	clog.Tracef("replaces: %x, ref: %s", f.replaces, f.writeINodeRef)

	promFileSyncs.WithLabelValues(f.volume.Name).Inc()
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
			clog.Errorf("sync: unexpected update error: %s", err)
			return err
		}
		// We can write a smarter merge function -- O_APPEND for example, doing the
		// right thing, by keeping some state in the file and actually appending it.
		// Today, it's Last Write Wins.
		promFileChangedSyncs.WithLabelValues(f.volume.Name).Inc()
		oldINode := f.inode
		f.inode, err = f.srv.inodes.GetINode(f.srv.getContext(), replaced)
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

	err = mds.SetFileEntry(f.path, &models.FileEntry{
		Chain: f.inode.Chain,
	})

	newLive := f.getLiveINodes()
	var dead *roaring.Bitmap
	// Cleanup.

	// TODO(barakmich): Correct behavior depending on O_CREAT
	dead = roaring.AndNot(f.initialINodes, newLive)
	if replaced.INode != 0 && f.replaces == 0 {
		deadinode, err := f.srv.inodes.GetINode(f.srv.getContext(), replaced)
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
	mds.ModifyDeadMap(f.writeINodeRef.Volume(), newLive, dead)

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
	if f.volume.Type != models.Volume_FILE {
		return
	}
	f.srv.decRef(f.volume.Name, f.initialINodes)
	if !closing {
		f.initialINodes = f.getLiveINodes()
		f.srv.incRef(f.volume.Name, f.initialINodes)
	}
	bm, _ := f.srv.getBitmap(f.volume.Name)
	card := uint64(0)
	if bm != nil {
		card = bm.GetCardinality()
	}
	promOpenINodes.WithLabelValues(f.volume.Name).Set(float64(card))
	mlog.Tracef("updating claim %s %s", f.volume.Name, bm)
	err := f.srv.fsMDS().ClaimVolumeINodes(f.srv.lease, agro.VolumeID(f.volume.Id), bm)
	if err != nil {
		mlog.Error("file: TODO: Can't re-claim")
	}
}

func (f *file) Truncate(size int64) error {
	if f.readOnly {
		return os.ErrPermission
	}
	return f.fileHandle.Truncate(size)
}
