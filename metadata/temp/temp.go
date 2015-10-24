package local

import (
	"errors"
	"strings"
	"sync"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/types"
)

func init() {
	agro.RegisterMetadataProvider("temp", newTempMetadata)
}

type temp struct {
	mut     sync.Mutex
	inode   uint64
	volumes map[string]dir
}

type dir struct {
	path    string
	subdirs map[string]dir
	data    *types.Directory
}

func makedir(path string) dir {
	return dir{
		path:    path,
		subdirs: make(map[string]dir),
	}
}

func newTempMetadata(address string) agro.Metadata {
	return &temp{
		volumes: make(map[string]dir),
	}
}

func (t *temp) Mkfs() error { return nil }

func (t *temp) CreateVolume(volume string) error {
	t.volumes[volume] = makedir("/")
	return nil
}

func (t *temp) CommitInodeIndex() (uint64, error) {
	t.mut.Lock()
	defer t.mut.Unlock()
	t.inode++
	return t.inode, nil
}

func (t *temp) Mkdir(path agro.Path, dir *types.Directory) error {
	if path.Path == "/" {
		return errors.New("Can't create the root directory")
	}
	root, ok := t.volumes[path.Volume]
	if !ok {
		return errors.New("ENOENT")
	}
	part := strings.Split(path.Path, "/")
	part = part[1:]
	d := root
	for i, p := range part {
		newdir, ok := d.subdirs[p]
		if !ok {
			if i != len(part)-1 {
				return errors.New("cannot create directory recursively")
			}
			newdir = makedir(d.path + "/" + p)
			d.subdirs[p] = newdir
			break
		}
		d = newdir
	}
	return nil
}

func (t *temp) Getdir(path agro.Path) (*types.Directory, []agro.Path, error) {
	root, ok := t.volumes[path.Volume]
	if !ok {
		return nil, nil, errors.New("EBADF")
	}
	dir := root
	if path.Path != "/" {
		part := strings.Split(path.Path, "/")
		part = part[1:]
		for _, p := range part {
			newdir, ok := dir.subdirs[p]
			if !ok {
				return nil, nil, errors.New("cannot find directory")
			}
			dir = newdir
		}
	}
	var subdirs []agro.Path
	for k := range dir.subdirs {
		subdirs = append(subdirs, agro.Path{
			Volume: path.Volume,
			Path:   path.Path + "/" + k,
		})
	}
	return dir.data, subdirs, nil
}
