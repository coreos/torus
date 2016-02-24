package server

import (
	"os"

	"golang.org/x/net/context"

	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/agro"
	"github.com/coreos/agro/blockset"
	"github.com/coreos/agro/models"
)

func (s *server) Rename(from, to agro.Path) error {
	if from.Volume != to.Volume {
		return agro.ErrInvalid
	}
	newINodeID, err := s.mds.CommitINodeIndex(from.Volume)
	if err != nil {
		return err
	}
	inode, _, err := s.updateINodeChain(
		s.getContext(),
		from,
		func(inode *models.INode, vol agro.VolumeID) (*models.INode, agro.INodeRef, error) {
			if inode == nil {
				return nil, agro.NewINodeRef(vol, newINodeID), os.ErrNotExist
			}
			inode.INode = uint64(newINodeID)
			var newFilenames []string
			for _, x := range inode.Filenames {
				if x == from.Path {
					newFilenames = append(newFilenames, to.Path)
					continue
				}
				newFilenames = append(newFilenames, x)
			}
			inode.Filenames = newFilenames
			return inode, agro.NewINodeRef(vol, newINodeID), nil
		})
	if err != nil {
		return err
	}
	err = s.mds.SetFileEntry(from, &models.FileEntry{})
	if err != nil {
		return err
	}
	return s.mds.SetFileEntry(to, &models.FileEntry{
		Chain: inode.Chain,
	})
}

func (s *server) Link(p agro.Path, new agro.Path) error {
	if p.Volume != new.Volume {
		return agro.ErrInvalid
	}
	newINodeID, err := s.mds.CommitINodeIndex(p.Volume)
	if err != nil {
		return err
	}
	clog.Debugf("link %s to %s", p, new)
	inode, replaced, err := s.updateINodeChain(
		s.getContext(),
		p,
		func(inode *models.INode, vol agro.VolumeID) (*models.INode, agro.INodeRef, error) {
			if inode == nil {
				return nil, agro.NewINodeRef(vol, newINodeID), os.ErrNotExist
			}
			inode.INode = uint64(newINodeID)
			inode.Filenames = append(inode.Filenames, new.Path)
			return inode, agro.NewINodeRef(vol, newINodeID), nil
		})
	if err != nil {
		return err
	}
	clog.Debugf("newinode %s replaced %s", inode, replaced)
	return s.mds.SetFileEntry(new, &models.FileEntry{
		Chain: inode.Chain,
	})
}

func (s *server) Symlink(to string, new agro.Path) error {
	_, ent, err := s.FileEntryForPath(new)
	if err != nil && err != os.ErrNotExist {
		return err
	}
	if err != os.ErrNotExist {
		clog.Error(ent, err)
		if ent.Chain != 0 {
			return agro.ErrExists
		}
	}
	return s.mds.SetFileEntry(new, &models.FileEntry{
		Sympath: to,
	})
}

func (s *server) removeFile(p agro.Path) error {
	clog.Debugf("removing file %s", p)
	vol, ent, err := s.FileEntryForPath(p)
	if err != nil {
		return err
	}
	if ent.Sympath != "" {
		return s.mds.SetFileEntry(p, &models.FileEntry{})
	}
	ref, err := s.mds.GetChainINode(p.Volume, agro.NewINodeRef(vol, agro.INodeID(ent.Chain)))
	if err != nil {
		return err
	}
	inode, err := s.inodes.GetINode(context.TODO(), ref)
	if err != nil {
		return err
	}

	var newFilenames []string
	for _, x := range inode.Filenames {
		if x != p.Path {
			newFilenames = append(newFilenames, x)
		}
	}
	// If we're not completely deleting it, and this is not a symlink
	if len(newFilenames) != 0 && len(newFilenames) != len(inode.Filenames) {
		// Version the INode, move the chain forward, but leave the data.
		newINode, err := s.mds.CommitINodeIndex(p.Volume)
		if err != nil {
			return err
		}
		_, _, err = s.updateINodeChain(
			s.getContext(),
			p,
			func(inode *models.INode, vol agro.VolumeID) (*models.INode, agro.INodeRef, error) {
				if inode == nil {
					return nil, agro.NewINodeRef(vol, newINode), os.ErrNotExist
				}
				inode.INode = uint64(newINode)
				inode.Filenames = newFilenames
				return inode, agro.NewINodeRef(vol, newINode), nil
			})
		if err != nil {
			return err
		}
	}
	err = s.mds.SetFileEntry(p, &models.FileEntry{})
	if err != nil {
		return err
	}

	if len(newFilenames) == 0 {
		// Clean up after ourselves.
		bs, err := blockset.UnmarshalFromProto(inode.Blocks, s.blocks)
		if err != nil {
			return err
		}
		live := bs.GetLiveINodes()
		// Anybody who had it open still does, and a write/sync will bring it back,
		// as expected. So this is safe to modify.
		err = s.mds.SetChainINode(p.Volume, agro.NewINodeRef(ref.Volume(), agro.INodeID(inode.Chain)), ref, agro.NewINodeRef(0, 0))
		if err != nil {
			return err
		}
		return s.mds.ModifyDeadMap(ref.Volume(), roaring.NewBitmap(), live)
	}
	return nil
}
