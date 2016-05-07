package block

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"golang.org/x/net/context"
)

type BlockFile struct {
	*agro.File
	vol *BlockVolume
}

func (s *BlockVolume) OpenBlockFile() (*BlockFile, error) {
	if s.volume.Type != VolumeType {
		panic("Wrong type")
	}
	err := s.mds.Lock(s.srv.Lease())
	if err != nil {
		return nil, err
	}
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
		return nil, agro.ErrNotExist
	}
	ref := agro.INodeRefFromBytes(found.INodeRef)
	inode, err := s.getOrCreateBlockINode(ref)
	if err != nil {
		return nil, err
	}
	bs, err := blockset.UnmarshalFromProto(inode.GetBlocks(), s.srv.Blocks)
	if err != nil {
		return nil, err
	}
	f, err := s.srv.CreateFile(s.volume, inode, bs)
	f.ReadOnly = true
	return &BlockFile{
		File: f,
		vol:  s,
	}, nil
}

func (f *BlockFile) Close() error {
	err := f.Sync()
	if err != nil {
		return err
	}
	err = f.File.Close()
	if err != nil {
		return err
	}
	return f.vol.mds.Unlock()
}

func (f *BlockFile) inodeContext() context.Context {
	return context.WithValue(context.TODO(), agro.CtxWriteLevel, agro.WriteAll)
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
