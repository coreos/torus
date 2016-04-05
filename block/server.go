package block

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/models"
	"golang.org/x/net/context"
)

type BlockVolume struct {
	srv    *agro.Server
	mds    blockMetadata
	volume *models.Volume
}

func CreateBlockVolume(s *agro.Server, volume string, size uint64) error {
	panic("TODO")
}

func OpenBlockVolume(s *agro.Server, volume string) (*BlockVolume, error) {
	vol, err := s.MDS.GetVolume(volume)
	if err != nil {
		return nil, err
	}
	mds, err := createBlockMetadata(s.MDS, agro.VolumeID(vol.Id))
	if err != nil {
		return nil, err
	}
	return &BlockVolume{
		srv:    s,
		mds:    mds,
		volume: vol,
	}, nil
}

func (s *BlockVolume) getContext() context.Context {
	return context.TODO()
}

func (s *BlockVolume) getOrCreateBlockINode(ref agro.INodeRef) (*models.INode, error) {
	if ref.Volume() != agro.VolumeID(s.volume.Id) {
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
	inode.Blocks, err = blockset.MarshalToProto(bs)
	return inode, err
}
