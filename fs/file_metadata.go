package fs

import (
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

func (s *server) modDirMetadata(p agro.Path, f func(md *models.Metadata) error) error {
	dir, _, err := s.fsMDS().Getdir(p)
	if err != nil {
		return err
	}
	err = f(dir.Metadata)
	if err != nil {
		return err
	}
	return s.fsMDS().ChangeDirMetadata(p, dir.Metadata)
}

func (s *server) Chmod(name agro.Path, mode os.FileMode) error {
	if name.IsDir() {
		return s.modDirMetadata(name, func(md *models.Metadata) error {
			md.Mode = uint32(mode)
			return nil
		})
	}
	// TODO(barakmich): Fix this hack
	for _, v := range s.openFileChains {
		for _, x := range v.fh.inode.Filenames {
			if x == name.Path {
				if v.fh.writeOpen {
					v.fh.inode.Permissions.Mode = uint32(mode)
					v.fh.changed["mode"] = true
					return nil
				}
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
		return s.modDirMetadata(name, func(md *models.Metadata) error {
			if uid >= 0 {
				md.Uid = uint32(uid)
			}
			if gid >= 0 {
				md.Gid = uint32(gid)
			}
			return nil
		})
	}
	return s.modFileMetadata(name, func(inode *models.INode) error {
		if uid >= 0 {
			inode.Permissions.Uid = uint32(uid)
		}
		if gid >= 0 {
			inode.Permissions.Gid = uint32(gid)
		}
		return nil
	})
}
