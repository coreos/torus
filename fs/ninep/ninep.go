package ninep

import (
	"bytes"
	"errors"
	"hash/fnv"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"time"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/coreos/agro/server"
	"github.com/coreos/pkg/capnslog"

	// TODO(mischief): use a better 9p package
	"github.com/lionkov/go9p/p"
	"github.com/lionkov/go9p/p/srv"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/agro", "ninep")

// AgroFid holds context for files and directories in the 9p server.
// If it is a directory, or a file which is not opened, File will be nil.
// If it is a directory, Path.IsDir() will be true.
type AgroFid struct {
	agro.File
	agro.Path
}

// TODO(mischief): allow clean termination
func ListenAndServe(addr string, sv agro.FSServer) error {
	fs, err := New(sv)
	if err != nil {
		return err
	}

	return fs.StartNetListener("tcp", addr)
}

func New(sv agro.FSServer) (*AgroFS, error) {
	fs := &AgroFS{
		Srv: srv.Srv{
			// use the os users for now, agro has no user db.
			Upool: p.OsUsers,

			// 64K seems like a nice size
			Msize: math.MaxUint16,

			// we do dotu
			Dotu: true,
		},
		dfs: sv,
	}

	// print fcalls if we're tracing
	if clog.LevelAt(capnslog.TRACE) {
		fs.Srv.Debuglevel = srv.DbgPrintFcalls
	}

	if !fs.Start(fs) {
		return nil, errors.New("AgroFS does not implement srv.ReqOps!")
	}

	return fs, nil
}

type AgroFS struct {
	srv.Srv

	dfs agro.FSServer
}

func (fs *AgroFS) mkqid(apath agro.Path, d os.FileInfo) *p.Qid {
	clog.Tracef("mkqid path %s fileinfo %#v", apath, d)

	var qid p.Qid

	if d != nil {
		agrofi := d.Sys().(server.FileInfo)
		qid.Version = uint32(d.ModTime().UnixNano() / 1000000)

		if !apath.IsDir() {
			qid.Path = uint64(agrofi.Ref.INode)
		}
	}

	// files have nodes and directories don't, so we fake a qid.Path with a
	// hash for directories.
	if apath.IsDir() {
		qid.Type |= p.QTDIR
		hash := fnv.New64a()
		io.WriteString(hash, path.Clean(apath.Path))
		qid.Path = hash.Sum64()
	}

	return &qid
}

func (fs *AgroFS) mkmode(apath agro.Path, d os.FileInfo) uint32 {
	clog.Tracef("mkmode path %s name %q size %d mode %s modtime %q isdir %v",
		apath, d.Name(), d.Size(), d.Mode(), d.ModTime(), d.IsDir())

	ret := uint32(d.Mode() & 0777)

	if d.IsDir() {
		ret |= p.DMDIR
	}

	return ret
}

func (fs *AgroFS) Attach(req *srv.Req) {
	clog.Trace("attach")

	volume := req.Tc.Aname
	apath := agro.Path{volume, "/"}

	// use aname as volume, check it
	vols, err := fs.dfs.GetVolumes()
	if err != nil {
		req.RespondError(err)
		return
	}

	havevol := false
	for _, v := range vols {
		if v.Name == volume {
			havevol = true
		}
	}

	if !havevol {
		req.RespondError(srv.Enotdir)
		return
	}

	// TODO: stat / when it works
	/*
		st, err := fs.dfs.Lstat(apath)
		if err != nil {
			clog.Errorf("%s does not exist", apath)
			req.RespondError(srv.Enotdir)
			return
		}
	*/

	// for now we fake it

	fid := &AgroFid{
		Path: apath,
	}

	req.Fid.Aux = fid

	qid := fs.mkqid(apath, nil)
	req.RespondRattach(qid)
}

func (fs *AgroFS) Clunk(req *srv.Req) {
	clog.Trace("clunk")

	// TODO: remove on ORCLOSE, maybe File.Sync
	req.RespondRclunk()
}

func (fs *AgroFS) ConnClosed(conn *srv.Conn) {
	clog.Trace("connclosed")
}

func (fs *AgroFS) ConnOpened(conn *srv.Conn) {
	clog.Trace("connopened")
}

func okname(name string) bool {
	// no /
	if strings.Contains(name, "/") {
		return false
	}

	// no . or ..
	if name == "." || name == ".." {
		return false
	}

	return true
}

// error used for bad file names
var Ebadname = &p.Error{"invalid file name", p.EINVAL}

func (fs *AgroFS) Create(req *srv.Req) {
	fid := req.Fid.Aux.(*AgroFid)
	child, _ := fid.Child(req.Tc.Name)
	clog.Tracef("create %#v %q %s", fid, req.Tc.Name, os.FileMode(req.Tc.Perm))

	// if fid is not a dir, can't create in it
	if !fid.IsDir() {
		req.RespondError(srv.Enotdir)
		return
	}

	if !okname(req.Tc.Name) {
		req.RespondError(Ebadname)
		return
	}

	if req.Tc.Perm&p.DMDIR > 0 {
		// XXX: ensure child has / suffix so IsDir works, etc
		child.Path += "/"

		// exists?
		if _, err := fs.dfs.Readdir(child); err == nil {
			req.RespondError(srv.Eexist)
			return
		}

		nowsec := uint64(time.Now().Unix())

		// horrible hack
		uid := uint32(req.Fid.User.Id())
		gid := uint32(req.Fid.User.Groups()[0].Id())
		md := &models.Metadata{
			Uid:  uid,
			Gid:  gid,
			Mode: req.Tc.Perm,
			// XXX: what is this?
			Flags: 0,
			Ctime: nowsec,
			Mtime: nowsec,
		}

		if err := fs.dfs.Mkdir(child, md); err != nil {
			req.RespondError(err)
			return
		}

		fid.Path = child

		qid := fs.mkqid(child, nil)
		req.RespondRcreate(qid, 0)
		return
	}

	nowsec := uint64(time.Now().Unix())

	// horrible hack
	uid := uint32(req.Fid.User.Id())
	gid := uint32(req.Fid.User.Groups()[0].Id())

	md := &models.Metadata{
		Uid:  uid,
		Gid:  gid,
		Mode: req.Tc.Perm,
		// XXX: what is this?
		Flags: 0,
		Ctime: nowsec,
		Mtime: nowsec,
	}

	// os.O_EXCL to ensure it doesn't exist on create
	f, err := fs.dfs.OpenFileMetadata(child, os.O_CREATE|os.O_EXCL, md)
	if err != nil {
		// facepalm
		if err.Error() == os.ErrExist.Error() {
			req.RespondError(srv.Eexist)
		} else {
			req.RespondError(err)
		}
		return
	}

	st, err := f.Stat()
	if err != nil {
		f.Close()
		req.RespondError(err)
	}

	fid.File = f
	fid.Path = child

	qid := fs.mkqid(child, st)
	req.RespondRcreate(qid, 0)
}

func (fs *AgroFS) FidDestroy(sfid *srv.Fid) {
	clog.Tracef("fiddestroy %T %+v", sfid.Aux, sfid.Aux)

	if sfid.Aux == nil {
		return
	}

	fid := sfid.Aux.(*AgroFid)
	if !fid.IsDir() && fid.File != nil {
		fid.Close()
	}
}

func (fs *AgroFS) Flush(req *srv.Req) {
	clog.Trace("flush")

	// TODO: cancel a pending read/write. etc
	// this will probably involve using net/context somehow.
	req.Flush()
}

func (fs *AgroFS) permcheck(fi os.FileInfo, mode uint8, user p.User) bool {
	perm := fi.Mode() & os.ModePerm

	afi := fi.Sys().(server.FileInfo)
	ino := afi.INode
	perms := ino.Permissions

	clog.Tracef("permcheck: perm %o mode %x (fi perms %#v)", perm, mode, perms)

	if perms.Uid == uint32(user.Id()) {
		perm = (perm >> 6)
	} else if user.IsMember(fs.Upool.Gid2Group(int(perms.Gid))) {
		perm = (perm >> 3)
	}

	switch mode & 3 {
	case p.OREAD:
		return (perm & 4) != 0
	case p.OWRITE:
		return (perm & 2) != 0
	case p.OEXEC:
		return (perm & 1) != 0
	case p.ORDWR:
		return (perm & 6) == 6
	}

	return false
}

func (fs *AgroFS) Open(req *srv.Req) {
	fid := req.Fid.Aux.(*AgroFid)
	mode := req.Tc.Mode
	clog.Tracef("open %#v mode %x", fid, mode)

	if fid.IsDir() {
		// TODO: perm check for dirs
		qid := fs.mkqid(fid.Path, nil)
		req.RespondRopen(qid, 0)
		return
	}

	// TODO: eventually perm check will be implemented by agro for files
	// for now we have our own down below.
	f, err := fs.dfs.OpenFile(fid.Path, int(mode), 0)
	if err != nil {
		req.RespondError(err)
		return
	}

	st, err := f.Stat()
	if err != nil {
		f.Close()
		req.RespondError(err)
		return
	}

	if !fs.permcheck(st, mode, req.Fid.User) {
		f.Close()
		req.RespondError(srv.Eperm)
		return
	}

	fid.File = f

	qid := fs.mkqid(fid.Path, st)

	req.RespondRopen(qid, 0)
}

func (fs *AgroFS) packdir(apath agro.Path, owner string) (*p.Dir, error) {
	dir := new(p.Dir)

	if !apath.IsDir() {
		fi, err := fs.dfs.Lstat(apath)
		if err != nil {
			return nil, err
		}

		dir.Qid = *fs.mkqid(apath, fi)
		dir.Mode = fs.mkmode(apath, fi)

		// TODO: use real mtime once agro fixes it
		dir.Mtime = uint32(fi.ModTime().Unix())
		dir.Mtime = uint32(time.Now().Unix())
		dir.Atime = dir.Mtime

		dir.Length = uint64(fi.Size())
		// XXX: can't send rooted paths to 9p client
		dir.Name = path.Base(fi.Name())

		// fill in owner info
		afi := fi.Sys().(server.FileInfo)
		ino := afi.INode
		perms := ino.Permissions

		user := fs.Upool.Uid2User(int(perms.Uid))
		dir.Uid = user.Name()
		dir.Uidnum = perms.Uid
		dir.Muid = user.Name()
		dir.Muidnum = perms.Uid

		// XXX: OsUsers groupnames don't actually work
		//group := fs.Upool.Gid2Group(int(perms.Gid))
		dir.Gid = "agro" //group.Name()
		dir.Gidnum = perms.Gid
	} else {
		dir.Qid = *fs.mkqid(apath, nil)
		dir.Mode = 0775 | p.DMDIR
		// BUG: fix agro.Path
		dir.Name = path.Base(apath.Path)
		if dir.Name == "" {
			dir.Name = "/"
		}

		// TODO: owner info for dirs
		u := owner
		dir.Uid = u
		dir.Gid = u
		dir.Muid = u
	}

	return dir, nil
}

// readdir answers a request to read a dir. at some point it could be smarter,
// but for now just create the whole respond and snarf the requested area from
// it.
// XXX: fails with large directories.
func (fs *AgroFS) readdir(req *srv.Req) {
	fid := req.Fid.Aux.(*AgroFid)
	buf := new(bytes.Buffer)

	paths, err := fs.dfs.Readdir(fid.Path)
	if err != nil {
		req.RespondError(err)
		return
	}

	// TODO: get rid of this and use real owner for dirs
	owner := req.Fid.User.Name()

	var dirlen int64

	for _, ap := range paths {
		dir, err := fs.packdir(ap, owner)
		if err != nil {
			clog.Errorf("failed to pack dir %q: %v", ap, err)
			continue
		}

		clog.Tracef("packdir %#v", dir)

		pd := p.PackDir(dir, req.Conn.Dotu)
		n, _ := buf.Write(pd)
		dirlen += int64(n)
	}

	clog.Tracef("readdir offset %d count %d of %d", req.Tc.Offset, req.Tc.Count, buf.Len())

	rd := bytes.NewReader(buf.Bytes())
	count, err := rd.ReadAt(req.Rc.Data, int64(req.Tc.Offset))
	if err != nil && err != io.EOF {
		req.RespondError(err)
		return
	}

	p.SetRreadCount(req.Rc, uint32(count))
	req.Respond()
}

func (fs *AgroFS) Read(req *srv.Req) {
	rc := req.Rc
	tc := req.Tc

	fid := req.Fid.Aux.(*AgroFid)

	clog.Tracef("read %#v", fid)

	p.InitRread(rc, tc.Count)

	if fid.IsDir() {
		fs.readdir(req)
		return
	}

	count, err := fid.ReadAt(rc.Data, int64(tc.Offset))
	if err != nil && err != io.EOF {
		req.RespondError(err)
		return
	}

	p.SetRreadCount(rc, uint32(count))
	req.Respond()
}

func (fs *AgroFS) Remove(req *srv.Req) {
	fid := req.Fid.Aux.(*AgroFid)

	clog.Tracef("remove %#v", fid)

	// TODO(mischief): permcheck OWRITE in parent

	if err := fs.dfs.Remove(fid.Path); err != nil {
		req.RespondError(err)
		return
	}

	req.RespondRremove()
}

func (fs *AgroFS) Stat(req *srv.Req) {
	fid := req.Fid.Aux.(*AgroFid)

	clog.Tracef("stat %#v", fid)

	dir, err := fs.packdir(fid.Path, req.Fid.User.Name())
	if err != nil {
		req.RespondError(err)
		return
	}

	req.RespondRstat(dir)
}

func (fs *AgroFS) Walk(req *srv.Req) {
	fid := req.Fid.Aux.(*AgroFid)
	tc := req.Tc

	clog.Tracef("walk %q", tc.Wname)

	/*
		err := fid.stat()
		if err != nil {
			req.RespondError(err)
			return
		}
	*/

	wqids := make([]p.Qid, len(tc.Wname))
	apath := fid.Path
	i := 0
	for ; i < len(tc.Wname); i++ {
		p := agro.Path{
			Volume: apath.Volume,
			Path:   path.Join(apath.Path, tc.Wname[i]),
		}

		var fi os.FileInfo

		// can readdir a dir i suppose.
		dp := p
		dp.Path += "/"
		clog.Tracef("readdir %#v", dp)
		_, err := fs.dfs.Readdir(dp)
		if err == nil {
			// is a dir
			wqids[i] = *fs.mkqid(dp, fi)
			apath = dp
			continue
		}

		if !p.IsDir() {
			st, err := fs.dfs.Lstat(p)
			if err != nil {
				if i == 0 {
					req.RespondError(srv.Enoent)
					return
				}

				break
			}

			fi = st
		}

		wqids[i] = *fs.mkqid(p, fi)
		apath = p
	}

	if req.Newfid.Aux == nil {
		req.Newfid.Aux = new(AgroFid)
	}

	nfid := req.Newfid.Aux.(*AgroFid)
	nfid.Path = apath

	req.RespondRwalk(wqids[0:i])
}

func (fs *AgroFS) Write(req *srv.Req) {
	fid := req.Fid.Aux.(*AgroFid)

	clog.Tracef("write %#v", fid)

	if fid.IsDir() {
		req.RespondError(srv.Ebaduse)
		return
	}

	n, err := fid.WriteAt(req.Tc.Data, int64(req.Tc.Offset))
	if err != nil {
		req.RespondError(err)
		return
	}

	req.RespondRwrite(uint32(n))
}

// see http://man.cat-v.org/plan_9/5/stat for some details
//
// unfortunately, we don't try to rollback if an error happens in the middle of
// updating metadata.
func (fs *AgroFS) Wstat(req *srv.Req) {
	fid := req.Fid.Aux.(*AgroFid)
	dir := req.Tc.Dir

	clog.Tracef("wstat %#v dir %#v", fid, dir)

	if dir.ChangeIllegalFields() {
		req.RespondError(srv.Ebaduse)
		return
	}

	// change perms?
	if dir.ChangeMode() {
		// TODO: mode conversion
		if err := fs.dfs.Chmod(fid.Path, os.FileMode(dir.Mode)); err != nil {
			req.RespondError(err)
			return
		}
	}

	// change mtime?
	if dir.ChangeMtime() {
		// TODO: have a way to change mtime
		clog.Warning("tried to change mtime but have no impl!")
		//req.RespondError(srv.Enotimpl)
		//return
	}

	// change size?
	if dir.ChangeLength() {
		// TODO: truncate a file that is not open
		if fid.File == nil {
			req.RespondError(srv.Enotimpl)
			return
		}

		if err := fid.Truncate(int64(dir.Length)); err != nil {
			req.RespondError(err)
			return
		}
	}

	// renamed?
	if dir.ChangeName() {
		// TODO: update fid path?
		oldname := fid.Path
		newname := agro.Path{Volume: oldname.Volume, Path: path.Join(path.Dir(oldname.Path), dir.Name)}
		clog.Tracef("wstat rename %s -> %s", oldname, newname)
		if err := fs.dfs.Rename(oldname, newname); err != nil {
			req.RespondError(err)
			return
		}
	}

	// change gid?
	if dir.ChangeGID() {
		// TODO: have a way to change group
		req.RespondError(srv.Enotimpl)
		return
	}

	// stat(5) says:
	// As a special case, if all the elements of the
	// directory entry in a Twstat message are ``don't touch'' val-
	// ues, the server may interpret it as a request to guarantee
	// that the contents of the associated file are committed to
	// stable storage before the Rwstat message is returned.  (Con-
	// sider the message to mean, ``make the state of the file
	// exactly what it claims to be.'')
	if !dir.ChangeMode() && !dir.ChangeMtime() && !dir.ChangeLength() &&
		!dir.ChangeLength() && !dir.ChangeName() && !dir.ChangeGID() &&
		!fid.IsDir() && fid.File != nil {
		if err := fid.Sync(); err != nil {
			req.RespondError(err)
			return
		}
	}

	req.RespondRwstat()
}
