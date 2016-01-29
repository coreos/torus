package fuse

import (
	"errors"
	"io"
	"os"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/coreos/pkg/capnslog"
	"golang.org/x/net/context"

	"github.com/coreos/agro"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "fuse")

var fuseSrv *fs.Server

func MustMount(mountpoint, volume string, srv agro.Server) {
	os.Create("foo")
	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("agro"),
		fuse.Subtype("agrofs"),
		fuse.LocalVolume(),
		fuse.VolumeName(volume),
	)
	if err != nil {
		clog.Fatal(err)
	}
	defer c.Close()

	cfg := &fs.Config{}
	if clog.LevelAt(capnslog.TRACE) {
		cfg.Debug = func(msg interface{}) {
			clog.Trace(msg)
		}
	}
	fuseSrv = fs.New(c, cfg)
	err = fuseSrv.Serve(FS{srv, volume})
	if err != nil {
		clog.Fatal(err)
	}

	// Check if the mount process has an error to report.
	<-c.Ready
	if err := c.MountError; err != nil {
		clog.Fatal(err)
	}
}

type FS struct {
	dfs    agro.Server
	volume string
}

func (fs FS) Root() (fs.Node, error) {
	return Dir{dfs: fs.dfs, path: agro.Path{fs.volume, "/"}}, nil
}

type Dir struct {
	dfs    agro.Server
	path   agro.Path
	handle *DirHandle
}

type DirHandle struct {
	dfs  agro.Server
	path agro.Path
}

var _ fs.HandleReadDirAller = DirHandle{}

func (d Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	// TODO(jzelinskie): enable this when metadata is being utilized.
	/*
		fileInfo, err := d.dfs.Lstat(d.path)
		if err != nil {
			return err
		}

		a.Mtime = fileInfo.ModTime()
		a.Mode = fileInfo.Mode()
	*/

	a.Mode = os.ModeDir | 0555

	return nil
}

func (d Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	newPath, ok := d.path.Child(name)
	if !ok {
		return nil, errors.New("fuse: path is not a directory")
	}

	_, err := d.dfs.Lstat(newPath)
	if err == os.ErrNotExist {
		newPath.Path = newPath.Path + "/"
		_, err := d.dfs.Readdir(newPath)
		if err == os.ErrNotExist {
			return nil, fuse.ENOENT
		} else if err != nil {
			return nil, err
		}
		return Dir{dfs: d.dfs, path: newPath}, nil
	} else if err != nil {
		return nil, err
	}

	return File{dfs: d.dfs, path: newPath}, nil
}

func (d Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	dirstr := ""
	if req.Dir {
		dirstr = "/"
	}
	newPath, ok := d.path.Child(req.Name + dirstr)
	if !ok {
		return errors.New("fuse: path is not a directory")
	}
	return d.dfs.Remove(newPath)
}

func (d Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	newPath, ok := d.path.Child(req.Name + "/")
	if !ok {
		return nil, errors.New("fuse: path is not a directory")
	}
	err := d.dfs.Mkdir(newPath)
	if err != nil {
		return nil, err
	}
	return Dir{dfs: d.dfs, path: newPath}, nil
}

func (d Dir) Rename(ctx context.Context, req *fuse.RenameRequest, n fs.Node) error {
	oldpath, ok := d.path.Child(req.OldName)
	if !ok {
		return errors.New("fuse: path is a directory")
	}
	ndir := n.(Dir)
	newpath, ok := ndir.path.Child(req.NewName)
	if !ok {
		return errors.New("fuse: path is a directory")
	}
	err := d.dfs.Rename(oldpath, newpath)
	d.handle = nil
	ndir.handle = nil
	fuseSrv.InvalidateNodeData(d)
	return err
}

func (d DirHandle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	if !d.path.IsDir() {
		return nil, fuse.Errno(syscall.ENOTDIR)
	}

	paths, err := d.dfs.Readdir(d.path)
	if err != nil {
		clog.Error(err)
		return nil, err
	}

	var fuseEntries []fuse.Dirent
	for _, path := range paths {
		if path.IsDir() {
			fuseEntries = append(fuseEntries, fuse.Dirent{Name: path.Base(), Type: fuse.DT_Dir})
			continue
		}
		fuseEntries = append(fuseEntries, fuse.Dirent{Name: path.Filename(), Type: fuse.DT_File})
	}
	clog.Debugf("returning dirents: %#v", fuseEntries)
	return fuseEntries, nil
}

func (d Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	clog.Debugf("opening file at path %s", req.Name)
	newPath, ok := d.path.Child(req.Name)
	if !ok {
		clog.Error("not a dir")
		return nil, nil, errors.New("fuse: path is not a directory")
	}
	f, err := d.dfs.OpenFile(newPath, int(req.Flags)|os.O_CREATE, req.Mode)
	if err != nil {
		clog.Error(err)
		return nil, nil, err
	}
	clog.Debugf("path %s", newPath)
	node := File{
		dfs:  d.dfs,
		path: newPath,
	}
	fh := FileHandle{
		dfs:  d.dfs,
		path: newPath,
		file: f,
		node: &node,
	}
	return node, fh, nil
}

func (d Dir) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	if d.handle == nil {
		d.handle = &DirHandle{
			dfs:  d.dfs,
			path: d.path,
		}
	}
	return *d.handle, nil
}

type File struct {
	dfs  agro.Server
	path agro.Path
}

type FileHandle struct {
	dfs  agro.Server
	path agro.Path
	file agro.File
	node *File
}

var (
	_        fs.Node         = File{}
	_        fs.Handle       = FileHandle{}
	_        fs.HandleReader = FileHandle{}
	syncRefs                 = make(map[fuse.HandleID]agro.File)
)

func (f File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	clog.Debugf("opening %d %s", req.Node, req.Flags)
	var err error
	file, err := f.dfs.OpenFile(f.path, int(req.Flags), 0)
	if err != nil {
		clog.Error(err)
		return nil, err
	}
	out := FileHandle{
		dfs:  f.dfs,
		path: f.path,
		file: file,
		node: &f,
	}
	return out, nil
}

func (f File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	file := syncRefs[req.Handle]
	err := file.Sync()
	if err != nil {
		return err
	}
	delete(syncRefs, req.Handle)
	fuseSrv.InvalidateNodeAttr(f)
	return nil
}

func (fh FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	err := fh.file.Sync()
	if err != nil {
		return err
	}
	delete(syncRefs, req.Handle)
	fuseSrv.InvalidateNodeAttr(fh.node)
	return nil
}

func (fh FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	if req.Dir {
		return errors.New("ENOTDIR")
	}
	data := make([]byte, req.Size)
	n, err := fh.file.ReadAt(data, req.Offset)
	if err != nil && err != io.EOF {
		clog.Println(err)
		return err
	}
	resp.Data = data[:n]
	return nil
}

func (fh FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	syncRefs[req.Handle] = fh.file
	n, err := fh.file.WriteAt(req.Data, req.Offset)
	if err != nil && err != io.EOF {
		clog.Println(err)
		return err
	}
	resp.Size = n
	return nil
}

func (fh FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	clog.Debugf("closing %d nicely", req.Node)
	delete(syncRefs, req.Handle)
	return fh.file.Close()
}

func (f File) Attr(ctx context.Context, a *fuse.Attr) error {
	fileInfo, err := f.dfs.Lstat(f.path)
	if err != nil {
		return err
	}
	// TODO(jzelinskie): enable this when metadata is being utilized.
	/*

		a.Mtime = fileInfo.ModTime()
	*/

	a.Size = uint64(fileInfo.Size())
	a.Mode = fileInfo.Mode()

	return nil
}
