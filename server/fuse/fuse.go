package fuse

import (
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/server"
	"github.com/coreos/pkg/capnslog"
	"golang.org/x/net/context"

	"github.com/coreos/agro"
)

var (
	clog = capnslog.NewPackageLogger("github.com/coreos/agro", "fuse")
	flog = capnslog.NewPackageLogger("github.com/coreos/agro", "fusedbg")
)

var fuseSrv *fs.Server

func MustMount(mountpoint, volume string, srv agro.Server, rootMount bool) {
	options := []fuse.MountOption{
		fuse.FSName("agro"),
		fuse.Subtype("agrofs"),
		fuse.LocalVolume(),
		fuse.VolumeName(volume),
	}
	if rootMount {
		options = append(options, []fuse.MountOption{
			fuse.DefaultPermissions(),
			fuse.AllowOther(),
		}...)
	}
	c, err := fuse.Mount(
		mountpoint,
		options...,
	)
	if err != nil {
		clog.Fatal(err)
	}
	defer c.Close()

	cfg := &fs.Config{}
	if flog.LevelAt(capnslog.TRACE) {
		cfg.Debug = func(msg interface{}) {
			flog.Trace(msg)
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
	return &Dir{
		dfs:  fs.dfs,
		path: agro.Path{fs.volume, "/"},
	}, nil
}

type Dir struct {
	dfs    agro.Server
	path   agro.Path
	handle *DirHandle
}

type DirHandle struct {
	dfs  agro.Server
	path agro.Path
	dir  *Dir
}

var (
	_         fs.HandleReadDirAller = DirHandle{}
	cacheMut  sync.Mutex
	dirExists = make(map[agro.Path]error)
)

func readDirExists(dfs agro.Server, p agro.Path) ([]agro.Path, error) {
	cacheMut.Lock()
	defer cacheMut.Unlock()
	if v, ok := dirExists[p]; ok {
		return nil, v
	}
	_, err := dfs.Readdir(p)
	dirExists[p] = err
	return nil, err
}

func (d *Dir) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	stats, err := d.dfs.Statfs()
	if err != nil {
		return err
	}
	clog.Debug(stats)
	resp.Bsize = uint32(stats.BlockSize)
	return nil
}

func (d *Dir) ChangePath(p agro.Path) {
	d.path = p
}

func (d Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	fileInfo, err := d.dfs.Lstat(d.path)
	if err != nil {
		clog.Errorf("dir attr: %s", err)
		return err
	}

	fi := fileInfo.(server.FileInfo)
	a.Uid = fi.Dir.Metadata.Uid
	a.Gid = fi.Dir.Metadata.Gid
	a.Mtime = fileInfo.ModTime()
	a.Mode = os.ModeDir | fileInfo.Mode()
	return nil
}

func (d Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	return setattrHelper(ctx, req, resp, d.path, d.dfs)
}

func (d Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	newPath, ok := d.path.Child(name)
	if !ok {
		return nil, errors.New("fuse: path is not a directory")
	}

	_, err := readLstatExists(d.dfs, newPath)
	if err == os.ErrNotExist {
		newPath.Path = newPath.Path + "/"
		_, err := readDirExists(d.dfs, newPath)
		if err == os.ErrNotExist {
			return nil, fuse.ENOENT
		} else if err != nil {
			return nil, err
		}

		d := &Dir{dfs: d.dfs, path: newPath}
		pathRefs[newPath] = d
		return d, nil
	} else if err != nil {
		return nil, err
	}

	file := &File{
		dfs:  d.dfs,
		path: newPath,
	}
	pathRefs[newPath] = file
	return file, nil
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
	cacheMut.Lock()
	defer cacheMut.Unlock()
	delete(dirExists, d.path)
	delete(dirExists, newPath)
	delete(lstatExists, newPath)
	delete(lstatExists, d.path)
	return d.dfs.Remove(newPath)
}

func (d Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	newPath, ok := d.path.Child(req.Name + "/")
	if !ok {
		return nil, errors.New("fuse: path is not a directory")
	}
	t := uint64(time.Now().UnixNano())
	err := d.dfs.Mkdir(newPath, &models.Metadata{
		Uid:   req.Uid,
		Gid:   req.Gid,
		Mode:  uint32(req.Mode | os.ModeDir),
		Ctime: t,
		Mtime: t,
	})
	if err != nil {
		return nil, err
	}
	newd := &Dir{dfs: d.dfs, path: newPath}
	pathRefs[newPath] = newd
	cacheMut.Lock()
	defer cacheMut.Unlock()
	delete(dirExists, d.path)
	delete(dirExists, newPath)
	delete(lstatExists, newPath)
	delete(dirExists, d.path)
	return newd, nil
}

func (d Dir) Rename(ctx context.Context, req *fuse.RenameRequest, n fs.Node) error {
	oldpath, ok := d.path.Child(req.OldName)
	if !ok {
		return errors.New("fuse: path is a directory")
	}
	ndir := n.(*Dir)
	newpath, ok := ndir.path.Child(req.NewName)
	if !ok {
		return errors.New("fuse: path is a directory")
	}
	err := d.dfs.Rename(oldpath, newpath)
	if err != nil {
		return err
	}
	pathRefs[newpath] = pathRefs[oldpath]
	delete(pathRefs, oldpath)
	pathRefs[newpath].ChangePath(newpath)
	d.handle = nil
	ndir.handle = nil
	fuseSrv.InvalidateNodeData(d)
	cacheMut.Lock()
	defer cacheMut.Unlock()
	delete(dirExists, newpath)
	delete(lstatExists, newpath)
	return err
}

func (d Dir) Link(ctx context.Context, req *fuse.LinkRequest, old fs.Node) (fs.Node, error) {
	newpath, ok := d.path.Child(req.NewName)
	if !ok {
		clog.Error("not a dir")
		return nil, errors.New("fuse: path is a directory")
	}
	if _, ok := old.(*Dir); ok {
		clog.Error("can't hardlink a dir")
		return nil, errors.New("fuse: can't hardlink directory")
	}
	v := old.(*File)
	err := d.dfs.Link(v.path, newpath)
	if err != nil {
		clog.Error("link", err)
		return nil, err
	}

	f := &File{
		dfs:  d.dfs,
		path: newpath,
	}
	pathRefs[newpath] = f
	cacheMut.Lock()
	defer cacheMut.Unlock()
	delete(lstatExists, newpath)
	return f, nil
}

func (d Dir) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	newpath, ok := d.path.Child(req.NewName)
	if !ok {
		clog.Error("not a dir")
		return nil, errors.New("fuse: path is a directory")
	}
	err := d.dfs.Symlink(req.Target, newpath)
	if err != nil {
		clog.Error("symlink", err)
		return nil, err
	}

	f := &File{
		dfs:  d.dfs,
		path: newpath,
	}
	pathRefs[newpath] = f
	cacheMut.Lock()
	defer cacheMut.Unlock()
	delete(lstatExists, newpath)
	return f, nil
}

func (d DirHandle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	if !d.path.IsDir() {
		return nil, fuse.Errno(syscall.ENOTDIR)
	}

	paths, err := d.dfs.Readdir(d.path)
	if err != nil {
		clog.Error("readdirall", err)
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
	clog.Tracef("returning dirents: %s %#v", d.path, fuseEntries)
	return fuseEntries, nil
}

func (d Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	clog.Debugf("opening file at path %s", req.Name)
	newPath, ok := d.path.Child(req.Name)
	if !ok {
		clog.Error("not a dir")
		return nil, nil, errors.New("fuse: path is not a directory")
	}
	t := uint64(time.Now().UnixNano())
	f, err := d.dfs.OpenFileMetadata(newPath, int(req.Flags)|os.O_CREATE, &models.Metadata{
		Uid:   req.Uid,
		Gid:   req.Gid,
		Mode:  uint32(req.Mode),
		Ctime: t,
		Mtime: t,
	})
	if err != nil {
		clog.Error("create", err)
		return nil, nil, err
	}
	clog.Debugf("path %s", newPath)
	node := &File{
		dfs:  d.dfs,
		path: newPath,
	}

	cacheMut.Lock()
	defer cacheMut.Unlock()
	delete(lstatExists, newPath)
	pathRefs[newPath] = node
	fh := FileHandle{
		dfs:  d.dfs,
		path: newPath,
		file: f,
		node: node,
	}
	resp.Flags |= fuse.OpenDirectIO
	return node, fh, nil
}

func (d Dir) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	if d.handle == nil {
		d.handle = &DirHandle{
			dfs:  d.dfs,
			path: d.path,
			dir:  &d,
		}
	}
	return *d.handle, nil
}

func (d Dir) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	cacheMut.Lock()
	defer cacheMut.Unlock()
	delete(lstatExists, d.path)
	return nil
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
	_ fs.Node         = &File{}
	_ fs.Handle       = FileHandle{}
	_ fs.HandleReader = FileHandle{}

	syncMut     sync.Mutex
	syncRefs    = make(map[fuse.HandleID]agro.File)
	pathRefs    = make(map[agro.Path]pathChanger)
	lstatExists = make(map[agro.Path]error)
)

func readLstatExists(dfs agro.Server, p agro.Path) (os.FileInfo, error) {
	cacheMut.Lock()
	defer cacheMut.Unlock()
	if v, ok := lstatExists[p]; ok {
		return nil, v
	}
	_, err := dfs.Lstat(p)
	lstatExists[p] = err
	return nil, err
}

type pathChanger interface {
	fs.Node
	ChangePath(p agro.Path)
}

func (f *File) ChangePath(p agro.Path) {
	f.path = p
}

func (f File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	clog.Debugf("open: %d %s", req.Node, req.Flags)
	var err error
	cacheMut.Lock()
	defer cacheMut.Unlock()
	delete(lstatExists, f.path)
	file, err := f.dfs.OpenFile(f.path, int(req.Flags), 0)
	if err != nil {
		clog.Error("open:", f.path, err)
		return nil, err
	}
	out := FileHandle{
		dfs:  f.dfs,
		path: f.path,
		file: file,
		node: &f,
	}
	resp.Flags |= fuse.OpenDirectIO
	return out, nil
}

func (f File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	return setattrHelper(ctx, req, resp, f.path, f.dfs)
}

func setattrHelper(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse, p agro.Path, srv agro.Server) error {
	fileInfo, err := srv.Lstat(p)
	if err != nil {
		clog.Error("setattr:", p, err)
		return err
	}
	fi := fileInfo.(server.FileInfo)
	if req.Valid.Mode() {
		if fi.Symlink != "" {
			return errors.New("can't modify symlink")
		}
		err := srv.Chmod(p, req.Mode)
		if err != nil {
			return err
		}
		resp.Attr.Mode = req.Mode
	}
	if req.Valid.Uid() || req.Valid.Gid() {
		uid := -1
		gid := -1
		if req.Valid.Uid() {
			uid = int(req.Uid)
			resp.Attr.Uid = req.Uid
		}
		if req.Valid.Gid() {
			gid = int(req.Gid)
			resp.Attr.Gid = req.Gid
		}
		srv.Chown(p, uid, gid)
	}
	return nil
}

func (f File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	syncMut.Lock()
	defer syncMut.Unlock()
	file, ok := syncRefs[req.Handle]
	if !ok {
		return nil
	}
	err := file.Sync()
	if err != nil {
		clog.Error("fsync:", f.path, err)
		return err
	}
	delete(syncRefs, req.Handle)
	cacheMut.Lock()
	defer cacheMut.Unlock()
	delete(lstatExists, f.path)
	fuseSrv.InvalidateNodeAttr(f)
	return nil
}

func (f File) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	fileInfo, err := f.dfs.Lstat(f.path)
	if err != nil {
		return "", err
	}
	fi := fileInfo.(server.FileInfo)
	return fi.Symlink, nil
}

func (fh FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	err := fh.file.Sync()
	if err != nil {
		clog.Error("flush:", fh.path, err)
		return err
	}
	syncMut.Lock()
	defer syncMut.Unlock()
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
		clog.Error("read:", fh.path, err)
		return err
	}
	resp.Data = data[:n]
	return nil
}

func (fh FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	syncMut.Lock()
	defer syncMut.Unlock()
	syncRefs[req.Handle] = fh.file
	n, err := fh.file.WriteAt(req.Data, req.Offset)
	if err != nil && err != io.EOF {
		clog.Error("write:", fh.path, err)
		return err
	}
	resp.Size = n
	return nil
}

func (fh FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	clog.Tracef("closing %d nicely", req.Node)
	syncMut.Lock()
	defer syncMut.Unlock()
	delete(syncRefs, req.Handle)
	err := fh.file.Close()
	if err != nil {
		clog.Error("release:", fh.path, err)
	}
	return err
}

func (f File) Attr(ctx context.Context, a *fuse.Attr) error {
	fileInfo, err := f.dfs.Lstat(f.path)
	if err != nil {
		clog.Error("attr:", f.path, err)
		return err
	}
	// TODO(jzelinskie): enable this when metadata is being utilized.
	/*

		a.Mtime = fileInfo.ModTime()
	*/
	fi := fileInfo.(server.FileInfo)
	a.Uid = fi.INode.Permissions.Uid
	a.Gid = fi.INode.Permissions.Gid
	a.Size = uint64(fileInfo.Size())
	a.Mode = fileInfo.Mode()
	stats, err := f.dfs.Statfs()
	if err != nil {
		return err
	}
	a.BlockSize = uint32(stats.BlockSize)

	return nil
}
