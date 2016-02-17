package server

import (
	"errors"

	"github.com/coreos/agro"
)

func (s *server) Rename(from, to agro.Path) error {
	//TODO(barakmich): Handle hard links
	ref, err := s.inodeRefForPath(from)
	if err != nil {
		return err
	}
	clog.Debugf("renaming %s %s %#v", from, to, ref)
	_, err = s.mds.SetFileEntry(from, agro.NewINodeRef(0, 0))
	if err != nil {
		return err
	}
	_, err = s.mds.SetFileEntry(to, ref)
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
