package server

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/models"
)

var (
	_ agro.BlockServer = &server{}
)

func (s *server) Block() (agro.BlockServer, error) {
	return s, nil
}

func (s *server) CreateBlockVolume(volume string, size uint64) error {
	return s.mds.CreateVolume(&models.Volume{
		Name:     volume,
		MaxBytes: size,
		Type:     models.Volume_BLOCK,
	})
}

func (s *server) OpenBlockFile(volume string) (agro.BlockFile, error) {
	vol, err := s.mds.GetVolume(volume)
	if err != nil {
		return nil, err
	}
	if vol.Type != models.Volume_BLOCK {
		return nil, agro.ErrWrongVolumeType
	}
	mds := s.blockMDS()
	err = mds.LockBlockVolume(s.lease, agro.VolumeID(vol.Id))
	if err != nil {
		return nil, err
	}
	inode, err := mds.GetBlockVolumeINode(agro.VolumeID(vol.Id))
	if err != nil {
		return nil, err
	}
	bs, err := blockset.UnmarshalFromProto(inode.GetBlocks(), s.blocks)
	if err != nil {
		return nil, err
	}
	md, err := s.mds.GlobalMetadata()
	if err != nil {
		return nil, err
	}
	fh := &fileHandle{
		volume:  vol,
		inode:   inode,
		srv:     s,
		blocks:  bs,
		blkSize: int64(md.BlockSize),
	}
	f := &file{
		fileHandle: fh,
		readOnly:   false,
		writeOnly:  false,
	}
	return f, nil
}

func (f *file) blockSync(mds agro.BlockMetadataService) error {
	if !f.writeOpen {
		clog.Debugf("not syncing")
		return nil
	}
	clog.Debugf("Syncing block volume: %v", f.volume.Name)
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
	if f.inode.Volume != f.volume.Id {
		panic("mismatched volume and inode volume")
	}
	err = mds.SyncBlockVolume(f.inode)
	if err != nil {
		return err
	}
	f.writeOpen = false
	return nil
}
