package ninep

import (
	"bytes"
	"io"
	"math"
	"net"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/coreos/agro/server"

	"github.com/coreos/pkg/capnslog"

	"github.com/lionkov/go9p/p"
	"github.com/lionkov/go9p/p/clnt"
	"github.com/lionkov/go9p/p/srv"
)

var (
	testVolume = "test"

	// blah
	testUid  = os.Getuid()
	testUser = p.OsUsers.Uid2User(testUid)
)

func getClientServerPair(dotu bool) (*AgroFS, *clnt.Clnt, func()) {
	// XXX: seems like the wrong place for this.
	if testing.Verbose() {
		capnslog.SetGlobalLogLevel(capnslog.TRACE)
	} else {
		capnslog.SetGlobalLogLevel(capnslog.ERROR)
	}

	srv := server.NewMemoryServer()

	if err := srv.CreateVolume(testVolume); err != nil {
		panic(err)
	}

	fs, err := New(srv)
	if err != nil {
		panic(err)
	}

	srvend, clntend := net.Pipe()

	fs.NewConn(srvend)

	client, err := clnt.Connect(clntend, math.MaxUint16-p.IOHDRSZ, dotu)
	if err != nil {
		panic(err)
	}

	// setup root fid
	fid, err := client.Attach(nil, testUser, testVolume)
	if err != nil {
		panic("attach to " + testVolume + " failed: " + err.Error())
	}

	client.Root = fid

	closer := func() {
		// clunk root fid when we're done
		if err := client.Clunk(client.Root); err != nil {
			panic("clunking root fid failed: " + err.Error())
		}

		client.Unmount()
		srv.Close()
	}

	return fs, client, closer
}

func TestAttach(t *testing.T) {
	t.Parallel()
	_, cl, closer := getClientServerPair(true)
	defer closer()

	// attaching to an existing volume is already tested by getClientServerPair, so just try a non-existant one.
	_, err := cl.Attach(nil, testUser, "nonexistantvolume")
	if err == nil {
		t.Error("attaching to a non-existant volume was successful, but should not be")
	}
}

func TestCreate(t *testing.T) {
	t.Parallel()
	_, cl, closer := getClientServerPair(true)
	defer closer()

	for i, tt := range []struct {
		path    string
		perm    uint32
		mode    uint8
		wanterr bool
		err     error
	}{
		// make rw file
		{"file1", 0666, p.ORDWR, false, nil},
		// create file again should fail
		{"file1", 0666, p.ORDWR, true, srv.Eexist},

		// at least check that there are no complaints on create w/ OTRUNC
		{"file2", 0666, p.ORDWR | p.OTRUNC, false, nil},

		// executable by everyone, writeable by me/group
		{"file3", 0775, p.ORDWR, false, nil},

		// dir tests, hard coded for 0775 right now.
		{"dir1", 0775 | p.DMDIR, p.OREAD, false, nil},
		// create dir again should fail
		{"dir1", 0775 | p.DMDIR, p.OREAD, true, srv.Eexist},

		// bad names
		{".", 0666, p.ORDWR, true, Ebadname},
		{"..", 0666, p.ORDWR, true, Ebadname},
		// would need to use raw create for this
		//{"foo/bar", 0666, p.ORDWR, true, Ebadname},
	} {
		f, err := cl.FCreate(tt.path, tt.perm, tt.mode)
		if (err != nil) != tt.wanterr {
			t.Errorf("case %d: unexpected error state while creating %q %s %x: %v", i, tt.path, os.FileMode(tt.perm), tt.mode, err)
			continue
		}

		t.Logf("case %d: create %q %s %x -> %#v", i, tt.path, os.FileMode(tt.perm), tt.mode, err)

		if err != nil {
			if err.Error() != tt.err.Error() {
				t.Errorf("case %d: wrong error message: got %q, want %q", i, err.Error(), tt.err.Error())
				continue
			}
			continue
		}

		if err := f.Close(); err != nil {
			t.Errorf("case %d: failed to close (clunk) file %q: %v", i, tt.path, err)
		}

		dir, err := cl.FStat(tt.path)
		if err != nil {
			t.Errorf("case %d: failed to stat file %q: %v", i, tt.path, err)
			continue
		}

		// p.Dir.Mode -> tt.perm
		if dir.Mode != tt.perm {
			t.Errorf("case %d: wrong perms on %q: got %s want %s", i, tt.path, os.FileMode(dir.Mode), os.FileMode(tt.perm))
		}

		isdir := tt.perm&p.DMDIR > 0
		if (dir.Qid.Type == p.QTDIR) != isdir {
			t.Errorf("case %d: wrong qid type on %q: got %x", i, tt.path, dir.Qid.Type)
		}
	}
}

func TestOpen(t *testing.T) {
	t.Parallel()
	_, cl, closer := getClientServerPair(true)
	defer closer()

	for i, tt := range []struct {
		path    string
		perm    uint32 // to create with
		mode    uint8
		wanterr bool
		err     error
	}{
		// can open ro file ro
		{"good1", 0444, p.OREAD, false, nil},
		// can open write file write
		{"good2", 0222, p.OWRITE, false, nil},
		// can open rw file rw
		{"good3", 0666, p.ORDWR, false, nil},

		// can't open ro file rw
		{"bad1", 0444, p.ORDWR, true, srv.Eperm},
		// can't open write-only file for reading
		{"bad2", 0222, p.OREAD, true, srv.Eperm},
		// can't open no-perm file for writing or reading
		{"bad3", 0000, p.OREAD, true, srv.Eperm},
		{"bad4", 0000, p.OWRITE, true, srv.Eperm},
	} {
		f, err := cl.FCreate(tt.path, tt.perm, tt.mode)
		if err != nil {
			t.Errorf("case %d: error while creating %q %s %x: %v", i, tt.path, os.FileMode(tt.perm), tt.mode, err)
			continue
		}

		if err := f.Close(); err != nil {
			t.Errorf("case %d: failed to close (clunk) file %q: %v", i, tt.path, err)
			continue
		}

		f, err = cl.FOpen(tt.path, tt.mode)
		if (err != nil) != tt.wanterr {
			t.Errorf("case %d: unexpected error state while opening %q %s %x: %v", i, tt.path, os.FileMode(tt.perm), tt.mode, err)
			continue
		}

		t.Logf("case %d: open %q %x -> %#v", i, tt.path, tt.mode, err)

		if err != nil {
			if err.Error() != tt.err.Error() {
				t.Errorf("case %d: wrong error message: got %q, want %q", i, err.Error(), tt.err.Error())
				continue
			}
			continue
		}

		if err := f.Close(); err != nil {
			t.Errorf("case %d: failed to close (clunk) file %q: %v", i, tt.path, err)
			continue
		}
	}
}

func TestWriteRead(t *testing.T) {
	t.Parallel()
	_, cl, closer := getClientServerPair(true)
	defer closer()

	tt := struct {
		path string
		perm uint32 // to create with
		mode uint8
	}{"good1", 0666, p.ORDWR}

	// create file
	f, err := cl.FCreate(tt.path, tt.perm, tt.mode)
	if err != nil {
		t.Errorf("error while creating %q %s %x: %v", tt.path, os.FileMode(tt.perm), tt.mode, err)
		return
	}

	if err := f.Close(); err != nil {
		t.Errorf("failed to close (clunk) file %q: %v", tt.path, err)
		return
	}

	// open rw
	f, err = cl.FOpen(tt.path, tt.mode)
	if err != nil {
		t.Errorf("unexpected error state while opening %q %s %x: %v", tt.path, os.FileMode(tt.perm), tt.mode, err)
		return
	}

	// write a bunch of junk that is bigger than msize
	buf := make([]byte, 2*math.MaxUint16)
	fluff := []byte{'j', 'u', 'n', 'k', 0x0, 0xF, 'l', 'u', 0xF, 0xF}

	for i := 0; i < len(buf)/len(fluff); i++ {
		copy(buf[i*len(fluff):], fluff)
	}

	n, err := f.Writen(buf, 0)
	if err != nil {
		t.Errorf("failed to write %d bytes of data: %v", len(buf), err)
		return
	}

	if n != len(buf) {
		t.Errorf("failed to write exactly %d bytes of data, wrote %d", len(buf), n)
		return
	}

	if err := f.Close(); err != nil {
		t.Errorf("failed to close (clunk) file %q: %v", tt.path, err)
		return
	}

	// open ro
	f, err = cl.FOpen(tt.path, p.OREAD)
	if err != nil {
		t.Errorf("unexpected error state while opening %q %s %x: %v", tt.path, os.FileMode(tt.perm), tt.mode, err)
		return
	}

	// read back the data
	readbuf := make([]byte, len(buf))

	n, err = f.Readn(readbuf, 0)
	if err != nil {
		t.Errorf("failed to read %d bytes of data: %v", len(readbuf), err)
		return
	}

	if n != len(readbuf) {
		t.Errorf("failed to read exactly %d bytes of data, read %d", len(readbuf), n)
		return
	}

	// same?
	if !bytes.Equal(buf, readbuf) {
		t.Error("data written not the same as data read!")
		return
	}

	if err := f.Close(); err != nil {
		t.Errorf("failed to close (clunk) file %q: %v", tt.path, err)
		return
	}
}

func TestRemove(t *testing.T) {
	t.Parallel()
	_, cl, closer := getClientServerPair(true)
	defer closer()

	for i, tt := range []struct {
		path    string
		perm    uint32
		mode    uint8
		create  bool
		wanterr bool
		err     error
	}{
		{"existfile", 0666, p.ORDWR, true, false, nil},
		{"existdir", 0775 | p.DMDIR, p.OREAD, true, false, nil},
		{"dontexist", 0666, p.ORDWR, false, true, srv.Enoent},
	} {
		if tt.create {
			f, err := cl.FCreate(tt.path, tt.perm, tt.mode)
			if err != nil {
				t.Errorf("case %d: unexpected error state while creating %q %s %x: %v", i, tt.path, os.FileMode(tt.perm), tt.mode, err)
				continue
			}

			if err := f.Close(); err != nil {
				t.Errorf("case %d: failed to close (clunk) file %q: %v", i, tt.path, err)
			}
		}

		err := cl.FRemove(tt.path)
		if (err != nil) != tt.wanterr {
			t.Errorf("case %d: unexpected error state while removing %q %s %x: %v", i, tt.path, os.FileMode(tt.perm), tt.mode, err)
			continue
		}

		if err != nil {
			if err.Error() != tt.err.Error() {
				t.Errorf("case %d: wrong error message: got %q, want %q", i, err.Error(), tt.err.Error())
				continue
			}
			continue
		}

		// check that it really doesn't exist
		_, err = cl.FStat(tt.path)
		if err == nil {
			t.Errorf("case %d: expected stat to fail: got nil, want err")
		}
	}
}

type dirsorter []*p.Dir

func (d dirsorter) Len() int           { return len(d) }
func (d dirsorter) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d dirsorter) Less(i, j int) bool { return strings.Compare(d[i].Name, d[j].Name) == -1 }

func TestReaddir(t *testing.T) {
	t.Parallel()
	_, cl, closer := getClientServerPair(true)
	defer closer()

	// sorted by filename so iterative comparison works down below
	files := []struct {
		path string
		perm uint32
		mode uint8
	}{
		{"dir1", 0775 | p.DMDIR, p.OREAD},
		{"file1", 0444, p.OREAD},
	}

	for i, tt := range files {
		f, err := cl.FCreate(tt.path, tt.perm, tt.mode)
		if err != nil {
			t.Errorf("case %d: unexpected error state while creating %q %s %x: %v", i, tt.path, os.FileMode(tt.perm), tt.mode, err)
			continue
		}

		if err := f.Close(); err != nil {
			t.Errorf("case %d: failed to close (clunk) file %q: %v", i, tt.path, err)
		}
	}

	root, err := cl.FOpen("/", p.OREAD)
	if err != nil {
		t.Errorf("failed to open root dir: %v", err)
		return
	}

	dirs, err := root.Readdir(0)
	if err != nil && err != io.EOF {
		t.Errorf("failed to read root dir: %v", err)
		return
	}

	if len(files) != len(dirs) {
		t.Errorf("count mismatch in readdir: want %d got %d", len(files), len(dirs))
		return
	}

	sort.Sort(dirsorter(dirs))

	for i, dir := range dirs {
		if dir.Name != files[i].path {
			t.Errorf("case %d: name mismatch, got %q want %q", i, files[i].path, dir.Name)
		}

		if dir.Mode != files[i].perm {
			t.Errorf("case %d: permission mismatch, got %s want %s", i, os.FileMode(files[i].perm), os.FileMode(dir.Mode))
		}
	}
}
