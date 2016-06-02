package block

import (
	"github.com/coreos/torus"
	"github.com/coreos/torus/blockset"
	"golang.org/x/net/context"
)

type BlockFile struct {
	*torus.File
	vol *BlockVolume
}

func (s *BlockVolume) OpenBlockFile() (file *BlockFile, err error) {
	if s.volume.Type != VolumeType {
		panic("Wrong type")
	}
	if err = s.mds.Lock(s.srv.Lease()); err != nil {
		return nil, err
	}
	defer func() {
		// If this function returns an error, attempt to release the lock.
		// TODO: Log unlock errors?
		if err != nil {
			s.mds.Unlock()
		}
	}()
	ref, err := s.mds.GetINode()
	if err != nil {
		return nil, err
	}
	inode, err := s.getOrCreateBlockINode(ref)
	if err != nil {
		return nil, err
	}
	bs, err := blockset.UnmarshalFromProto(inode.GetBlocks(), s.srv.Blocks)
	if err != nil {
		return nil, err
	}
	f, err := s.srv.CreateFile(s.volume, inode, bs)
	if err != nil {
		return nil, err
	}
	return &BlockFile{
		File: f,
		vol:  s,
	}, nil
}

func (s *BlockVolume) OpenSnapshot(name string) (*BlockFile, error) {
	if s.volume.Type != VolumeType {
		panic("wrong type")
	}
	snaps, err := s.mds.GetSnapshots()
	if err != nil {
		return nil, err
	}
	var found Snapshot
	for _, x := range snaps {
		if x.Name == name {
			found = x
			break
		}
	}
	if found.Name != name {
		return nil, torus.ErrNotExist
	}
	ref := torus.INodeRefFromBytes(found.INodeRef)
	inode, err := s.getOrCreateBlockINode(ref)
	if err != nil {
		return nil, err
	}
	bs, err := blockset.UnmarshalFromProto(inode.GetBlocks(), s.srv.Blocks)
	if err != nil {
		return nil, err
	}
	f, err := s.srv.CreateFile(s.volume, inode, bs)
	if err != nil {
		return nil, err
	}
	f.ReadOnly = true
	return &BlockFile{
		File: f,
		vol:  s,
	}, nil
}

func (f *BlockFile) Close() (err error) {
	defer func() {
		// No matter what attempt to release the lock.
		unlockErr := f.vol.mds.Unlock()
		if err == nil {
			// TODO: Log unlock errors if err is not nil?
			err = unlockErr
		}
	}()

	if err = f.Sync(); err != nil {
		return err
	}
	return f.File.Close()
}

func (f *BlockFile) inodeContext() context.Context {
	return context.WithValue(context.TODO(), torus.CtxWriteLevel, torus.WriteAll)
}

func (f *BlockFile) Sync() error {
	if !f.WriteOpen() {
		clog.Debugf("not syncing")
		return nil
	}
	clog.Debugf("Syncing block volume: %v", f.vol.volume.Name)
	err := f.File.SyncBlocks()
	if err != nil {
		return err
	}
	ref, err := f.File.SyncINode(f.inodeContext())
	if err != nil {
		return err
	}
	return f.vol.mds.SyncINode(ref)
}
