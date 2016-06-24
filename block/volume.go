package block

import (
	"github.com/coreos/torus"
	"github.com/coreos/torus/blockset"
	"github.com/coreos/torus/models"
	"golang.org/x/net/context"
)

const VolumeType = "block"

type BlockVolume struct {
	srv    *torus.Server
	mds    blockMetadata
	volume *models.Volume
}

func CreateBlockVolume(mds torus.MetadataService, volume string, size uint64) error {
	id, err := mds.NewVolumeID()
	if err != nil {
		return err
	}
	blkmd, err := createBlockMetadata(mds, volume, id)
	if err != nil {
		return err
	}
	return blkmd.CreateBlockVolume(&models.Volume{
		Name:     volume,
		Id:       uint64(id),
		Type:     VolumeType,
		MaxBytes: size,
	})
}

func OpenBlockVolume(s *torus.Server, volume string) (*BlockVolume, error) {
	vol, err := s.MDS.GetVolume(volume)
	if err != nil {
		return nil, err
	}
	mds, err := createBlockMetadata(s.MDS, vol.Name, torus.VolumeID(vol.Id))
	if err != nil {
		return nil, err
	}
	return &BlockVolume{
		srv:    s,
		mds:    mds,
		volume: vol,
	}, nil
}

func DeleteBlockVolume(mds torus.MetadataService, volume string) error {
	vol, err := mds.GetVolume(volume)
	if err != nil {
		return err
	}
	bmds, err := createBlockMetadata(mds, vol.Name, torus.VolumeID(vol.Id))
	if err != nil {
		return err
	}
	return bmds.DeleteVolume()
}

func (s *BlockVolume) ResizeBlockVolume(mds torus.MetadataService, volume string, size uint64) error {
	vol, err := mds.GetVolume(volume)
	if err != nil {
		return err
	}
	bmds, err := createBlockMetadata(mds, vol.Name, torus.VolumeID(vol.Id))
	if err != nil {
		return err
	}
	if err = bmds.Lock(s.srv.Lease()); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			s.mds.Unlock()
		}
	}()
	return bmds.ResizeVolume(size)
}

func (s *BlockVolume) SaveSnapshot(name string) error    { return s.mds.SaveSnapshot(name) }
func (s *BlockVolume) GetSnapshots() ([]Snapshot, error) { return s.mds.GetSnapshots() }
func (s *BlockVolume) DeleteSnapshot(name string) error  { return s.mds.DeleteSnapshot(name) }

func (s *BlockVolume) getContext() context.Context {
	return context.TODO()
}

func (s *BlockVolume) getOrCreateBlockINode(ref torus.INodeRef) (*models.INode, error) {
	if ref.Volume() != torus.VolumeID(s.volume.Id) {
		panic("ids managed by metadata didn't match, how is that possible?")
	}
	if ref.INode != 1 {
		return s.srv.INodes.GetINode(s.getContext(), ref)
	}
	globals, err := s.mds.GlobalMetadata()
	if err != nil {

	}
	bs, err := blockset.CreateBlocksetFromSpec(globals.DefaultBlockSpec, nil)
	if err != nil {
		return nil, err
	}
	nBlocks := (s.volume.MaxBytes / globals.BlockSize)
	if s.volume.MaxBytes%globals.BlockSize != 0 {
		nBlocks++
	}
	err = bs.Truncate(int(nBlocks), globals.BlockSize)
	if err != nil {
		return nil, err
	}
	inode := models.NewEmptyINode()
	inode.INode = 1
	inode.Volume = s.volume.Id
	inode.Filesize = s.volume.MaxBytes
	inode.Blocks, err = torus.MarshalBlocksetToProto(bs)
	return inode, err
}
