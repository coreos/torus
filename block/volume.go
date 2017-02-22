package block

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/coreos/pkg/progressutil"
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

func CreateBlockFromSnapshot(srv *torus.Server, origvol, origsnap, newvol string, progress bool) error {
	// open original snapshot
	srcVol, err := OpenBlockVolume(srv, origvol)
	if err != nil {
		return fmt.Errorf("couldn't open block volume %s: %v", origvol, err)
	}
	bfsrc, err := srcVol.OpenSnapshot(origsnap)
	if err != nil {
		return fmt.Errorf("couldn't open snapshot: %v", err)
	}
	size := bfsrc.Size()

	// create new volume
	err = CreateBlockVolume(srv.MDS, newvol, size)
	if err != nil {
		return fmt.Errorf("error creating volume %s: %v", newvol, err)
	}
	blockvolDist, err := OpenBlockVolume(srv, newvol)
	if err != nil {
		return fmt.Errorf("couldn't open block volume %s: %v", newvol, err)
	}
	bfdist, err := blockvolDist.OpenBlockFile()
	if err != nil {
		return fmt.Errorf("couldn't open blockfile %s: %v", newvol, err)
	}
	defer bfdist.Close()

	if progress {
		pb := progressutil.NewCopyProgressPrinter()
		pb.AddCopy(bfsrc, newvol, int64(size), bfdist)
		err := pb.PrintAndWait(os.Stderr, 500*time.Millisecond, nil)
		if err != nil {
			return fmt.Errorf("couldn't copy: %v", err)
		}
	} else {
		n, err := io.Copy(bfdist, bfsrc)
		if err != nil {
			return fmt.Errorf("couldn't copy: %v", err)
		}
		if n != int64(size) {
			return fmt.Errorf("copied size %d doesn't match original size %d", n, size)
		}
	}
	err = bfdist.Sync()
	if err != nil {
		return fmt.Errorf("couldn't sync: %v", err)
	}
	return nil
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
	globals := s.mds.GlobalMetadata()
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
