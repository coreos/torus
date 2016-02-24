package server

import (
	"errors"
	"os"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

func (s *server) modFileMetadata(p agro.Path, f func(inode *models.INode) error) error {
	newINodeID, err := s.mds.CommitINodeIndex(p.Volume)
	if err != nil {
		return err
	}
	_, _, err = s.updateINodeChain(
		s.getContext(),
		p,
		func(inode *models.INode, vol agro.VolumeID) (*models.INode, agro.INodeRef, error) {
			if inode == nil {
				return nil, agro.NewINodeRef(vol, newINodeID), os.ErrNotExist
			}
			err := f(inode)
			if err != nil {
				return nil, agro.NewINodeRef(vol, newINodeID), err
			}
			inode.INode = uint64(newINodeID)
			return inode, agro.NewINodeRef(vol, newINodeID), nil
		})
	return err
}

func (s *server) Chmod(name agro.Path, mode os.FileMode) error {
	if name.IsDir() {
		return errors.New("unimplemented")
	}
	for _, x := range s.openFiles {
		if x.path.Equals(name) {
			if x.writeOpen {
				x.inode.Permissions.Mode = uint32(mode)
				x.changed["mode"] = true
				return nil
			}
		}
	}
	return s.modFileMetadata(name, func(inode *models.INode) error {
		inode.Permissions.Mode = uint32(mode)
		return nil
	})
}

func (s *server) Chown(name agro.Path, uid, gid int) error {
	if name.IsDir() {
		return errors.New("unimplemented")
	}
	return s.modFileMetadata(name, func(inode *models.INode) error {
		inode.Permissions.Uid = uint32(uid)
		inode.Permissions.Gid = uint32(gid)
		return nil
	})
}
