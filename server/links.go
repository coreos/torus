package server

import (
	"errors"

	"github.com/coreos/agro"
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
	inode, err := s.updateINodeChain(from, func(inode *models.INode, vol agro.VolumeID) (*models.INode, agro.INodeRef, error) {
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
	err = s.mds.SetFileEntry(from, &models.FileEntry{})
	if err != nil {
		return err
	}
	err = s.mds.SetFileEntry(to, &models.FileEntry{
		Chain: inode.Chain,
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *server) Link(p agro.Path, new agro.Path) error {
	return errors.New("unimplemented")
}

func (s *server) Symlink(p agro.Path, new agro.Path) error {
	return errors.New("unimplemented")
}
