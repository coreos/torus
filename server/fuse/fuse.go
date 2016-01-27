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

func MustMount(mountpoint, volume string, srv agro.Server) {
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

	err = fs.Serve(c, FS{srv, volume})
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
	dfs  agro.Server
	path agro.Path
}

var _ fs.HandleReadDirAller = Dir{}

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
		return nil, fuse.ENOENT
	} else if err != nil {
		return nil, err
	}

	if newPath.IsDir() {
		return Dir{dfs: d.dfs, path: newPath}, nil
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

func (d Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	if !d.path.IsDir() {
		return nil, fuse.Errno(syscall.ENOTDIR)
	}

	paths, err := d.dfs.Readdir(d.path)
	if err != nil {
		return nil, err
	}

	var fuseEntries []fuse.Dirent
	for _, path := range paths {
		if path.IsDir() {
			fuseEntries = append(fuseEntries, fuse.Dirent{Name: path.Filename(), Type: fuse.DT_Dir})
		}
		fuseEntries = append(fuseEntries, fuse.Dirent{Name: path.Filename(), Type: fuse.DT_File})
	}

	return fuseEntries, nil
}

type File struct {
	dfs  agro.Server
	path agro.Path
	file agro.File
}

var (
	_ fs.Handle       = File{}
	_ fs.HandleReader = File{}
)

func (f File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	var err error
	file, err := f.dfs.Open(f.path)
	if err != nil {
		clog.Error(err)
		return nil, err
	}
	clog.Debugf("opening %d", req.Node)
	f.file = file
	return f, nil
}

func (f File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	if req.Dir {
		return errors.New("ENOTDIR")
	}
	data := make([]byte, req.Size)
	n, err := f.file.ReadAt(data, req.Offset)
	if err != nil && err != io.EOF {
		clog.Println(err)
		return err
	}
	resp.Data = data[:n]
	return nil
}

func (f File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	clog.Debugf("closing %d nicely", req.Node)
	f.file.Close()
	return nil
}

func (f File) Attr(ctx context.Context, a *fuse.Attr) error {
	fileInfo, err := f.dfs.Lstat(f.path)
	if err != nil {
		return err
	}
	// TODO(jzelinskie): enable this when metadata is being utilized.
	/*

		a.Mtime = fileInfo.ModTime()
		a.Mode = fileInfo.Mode()
	*/

	a.Size = uint64(fileInfo.Size())
	a.Mode = 0555

	return nil
}
