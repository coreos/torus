// +build integration

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/coreos/torus"
	fsck "github.com/coreos/torus/cmd/fsck.torus/lib"
	mkfs "github.com/coreos/torus/cmd/mkfs.torus/lib"
)

func stderr(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, strings.TrimSpace(msg)+"\n", args...)
}

var lodevice = ""

const torusBlockSize = 1024 * 512 // 512K

type deviceBlockTester struct {
	db          *deviceBlock
	backingFile string
}

func run(cmd string, args ...string) error {
	stderr("Running: %s %v", cmd, args)
	command := exec.Command(cmd, args...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	return command.Run()
}

func newDeviceBlockTester(deviceSize uint64) (*deviceBlockTester, error) {
	stderr("Creating new block device...")
	dbt := &deviceBlockTester{}
	f, err := ioutil.TempFile("", "torus")
	if err != nil {
		return nil, err
	}
	f.Close()
	dbt.backingFile = f.Name()
	err = run("dd", "if=/dev/urandom", "of="+dbt.backingFile, "bs=1024", fmt.Sprintf("count=%d", deviceSize/1024))
	if err != nil {
		return nil, err
	}

	stderr("Running: %s, %v", "losetup", "-f")
	lodeviceBytes, err := exec.Command("losetup", "-f").Output()
	if err != nil {
		return nil, err
	}
	lodevice = strings.TrimSpace(string(lodeviceBytes))

	err = run("losetup", lodevice, dbt.backingFile)
	if err != nil {
		return nil, err
	}
	stderr("formatting block device")
	err = mkfs.Mkfs(mkfs.DefaultBlockSize, true, lodevice)
	if err != nil {
		return nil, err
	}

	cfg := torus.Config{
		DataDir:         "",
		BlockDevice:     lodevice,
		StorageSize:     0,
		MetadataAddress: "",
		ReadCacheSize:   0,
		ReadLevel:       0,
		WriteLevel:      0,
		TLS:             nil,
	}
	meta := torus.GlobalMetadata{
		BlockSize:        torusBlockSize,
		DefaultBlockSpec: nil,
		INodeReplication: 0,
	}

	db, err := newBlockDeviceBlockStore(lodevice, cfg, meta)
	dbt.db = db.(*deviceBlock)

	return dbt, nil
}

func (d *deviceBlockTester) Close(t *testing.T) error {
	stderr("Checking for block device corruption...")
	err := fsck.Fsck(true, lodevice, mkfs.DefaultBlockSize)
	if err != nil {
		t.Errorf("fsck failed!")
	}

	stderr("Cleaning up block device")
	d.db.Close()
	err = exec.Command("losetup", "-d", lodevice).Run()
	if err != nil {
		return err
	}
	err = os.Remove(d.backingFile)
	if err != nil {
		return err
	}
	return nil
}

type operation interface {
	perform(d *deviceBlock) error
	String() string
}

type writeOp struct {
	b           torus.BlockRef
	data        []byte
	errExpected bool
}

func (w *writeOp) String() string {
	return fmt.Sprintf("write to block ref %q, errExpected=%t", w.b.String(), w.errExpected)
}

func (w *writeOp) perform(d *deviceBlock) error {
	err := d.WriteBlock(context.TODO(), w.b, w.data)
	if w.errExpected {
		if err == nil {
			return fmt.Errorf("didn't error when it was expected to")
		}
		return nil
	}
	if err != nil {
		return err
	}
	r := &readOp{w.b, w.data, false}
	return r.perform(d)
}

type readOp struct {
	b            torus.BlockRef
	expectedData []byte
	errExpected  bool
}

func (r *readOp) String() string {
	return fmt.Sprintf("read to block ref %q, errExpected=%t", r.b.String(), r.errExpected)
}

func (r *readOp) perform(d *deviceBlock) error {
	data, err := d.GetBlock(context.TODO(), r.b)
	if r.errExpected {
		if err == nil {
			return fmt.Errorf("didn't error when it was expected to")
		}
		return nil
	}
	if err != nil {
		return err
	}
	if !bytes.Equal(data, r.expectedData) {
		return fmt.Errorf("read operation didn't return expected data for block ref %q\nexpected first 8 bytes: %v\nactual first 8 bytes: %v", r.b.String(), r.expectedData[:8], data[:8])
	}
	return nil
}

type deleteOp struct {
	b           torus.BlockRef
	errExpected bool
}

func (e *deleteOp) String() string {
	return fmt.Sprintf("delete of block ref %q, errExpected=%t", e.b.String(), e.errExpected)
}

func (e *deleteOp) perform(d *deviceBlock) error {
	err := d.DeleteBlock(context.TODO(), e.b)
	if e.errExpected {
		if err == nil {
			return fmt.Errorf("didn't error when it was expected to")
		}
		return nil
	}
	if err != nil {
		return err
	}
	h := &hasOp{e.b, false, false}
	return h.perform(d)
}

type hasOp struct {
	b           torus.BlockRef
	expectedVal bool
	errExpected bool
}

func (h *hasOp) perform(d *deviceBlock) error {
	hasBlock, err := d.HasBlock(context.TODO(), h.b)
	if h.errExpected {
		if err == nil {
			return fmt.Errorf("didn't error when it was expected to")
		}
		return nil
	}
	if err != nil {
		return err
	}
	if hasBlock != h.expectedVal {
		return fmt.Errorf("has block operation returned %t instead of expected value of %t for block ref %q", hasBlock, h.expectedVal, h.b.String)
	}
	return nil
}

func (h *hasOp) String() string {
	return fmt.Sprintf("has check of block ref %q, expected=%t, errExpected=%t", h.b.String(), h.expectedVal, h.errExpected)
}

func TestBlockDeviceStorage(t *testing.T) {
	dbt, err := newDeviceBlockTester(1024 * 1024 * 1024) // 1 GiB
	if err != nil {
		t.Fatalf("newDeviceBlockTester: %v\n", err)
	}
	defer func() {
		err = dbt.Close(t)
		if err != nil {
			t.Fatalf("dbt.Close(): %v\n", err)
		}
	}()

	order := binary.LittleEndian
	zeros := make([]byte, torusBlockSize)
	data1 := make([]byte, torusBlockSize)
	order.PutUint64(data1[0:8], 0x10)
	data2 := make([]byte, torusBlockSize)
	order.PutUint64(data2[0:8], 0x100)
	data3 := make([]byte, torusBlockSize)
	order.PutUint64(data3[0:8], 0x1000)
	data4 := make([]byte, torusBlockSize)
	order.PutUint64(data4[0:8], 0x10000)
	data5 := make([]byte, torusBlockSize)
	order.PutUint64(data5[0:8], 0x100000)

	for _, o := range []operation{
		// None of these blocks should exist yet
		&hasOp{torus.BlockRefFromUint64s(1, 0, 0), false, false},
		&hasOp{torus.BlockRefFromUint64s(1, 0, 1), false, false},
		&hasOp{torus.BlockRefFromUint64s(1, 0, 2), false, false},
		&hasOp{torus.BlockRefFromUint64s(1, 0, 3), false, false},
		&hasOp{torus.BlockRefFromUint64s(1, 0, 4), false, false},

		// Write out some data
		&writeOp{torus.BlockRefFromUint64s(1, 0, 0), data1, false},
		&writeOp{torus.BlockRefFromUint64s(1, 0, 1), data2, false},
		&writeOp{torus.BlockRefFromUint64s(1, 0, 2), data3, false},
		&writeOp{torus.BlockRefFromUint64s(1, 0, 3), data4, false},
		&writeOp{torus.BlockRefFromUint64s(1, 0, 4), data5, false},

		// Overwrite the data
		&writeOp{torus.BlockRefFromUint64s(1, 0, 0), data2, false},
		&writeOp{torus.BlockRefFromUint64s(1, 0, 1), data3, false},
		&writeOp{torus.BlockRefFromUint64s(1, 0, 2), data4, false},
		&writeOp{torus.BlockRefFromUint64s(1, 0, 3), data5, false},
		&writeOp{torus.BlockRefFromUint64s(1, 0, 4), data1, false},

		// Now all of these blocks should exist
		&hasOp{torus.BlockRefFromUint64s(1, 0, 0), true, false},
		&hasOp{torus.BlockRefFromUint64s(1, 0, 1), true, false},
		&hasOp{torus.BlockRefFromUint64s(1, 0, 2), true, false},
		&hasOp{torus.BlockRefFromUint64s(1, 0, 3), true, false},
		&hasOp{torus.BlockRefFromUint64s(1, 0, 4), true, false},

		// And these blocks should still not exist
		&hasOp{torus.BlockRefFromUint64s(1, 0, 5), false, false},
		&hasOp{torus.BlockRefFromUint64s(1, 1, 0), false, false},
		&hasOp{torus.BlockRefFromUint64s(2, 0, 0), false, false},

		// Reading an invalid block should error
		&readOp{torus.BlockRefFromUint64s(2, 0, 0), nil, true},

		// A block that's all zeros should still exist
		&writeOp{torus.BlockRefFromUint64s(2, 0, 0), zeros, false},
		&hasOp{torus.BlockRefFromUint64s(2, 0, 0), true, false},

		// If we delete a block, it shouldn't exist and should error when read
		&deleteOp{torus.BlockRefFromUint64s(1, 0, 0), false},
		&hasOp{torus.BlockRefFromUint64s(1, 0, 0), false, false},
		&readOp{torus.BlockRefFromUint64s(1, 0, 0), nil, true},

		// Deleting an invalid block should error
		&deleteOp{torus.BlockRefFromUint64s(1, 0, 0), true},
	} {
		err := o.perform(dbt.db)
		if err != nil {
			t.Errorf("error encountered when performing %s\nerror: %v", o.String(), err)
		}
	}
}

func TestFillUpDevice(t *testing.T) {
	dbt, err := newDeviceBlockTester(1024 * 1024 * 100) // 100 MiB
	if err != nil {
		t.Fatalf("newDeviceBlockTester: %v\n", err)
	}
	defer func() {
		err = dbt.Close(t)
		if err != nil {
			t.Fatalf("dbt.Close(): %v\n", err)
		}
	}()

	order := binary.LittleEndian

	// We should be able to write dbt.db.NumBlocks() blocks to the device
	var i uint64
	for i = 1; i <= dbt.db.NumBlocks(); i++ {
		buf := make([]byte, torusBlockSize)
		order.PutUint64(buf[0:8], i)
		w := &writeOp{torus.BlockRefFromUint64s(i, i, i), buf, false}
		err := w.perform(dbt.db)
		if err != nil {
			t.Errorf("error encountered when performing %s\nerror: %v", w.String(), err)
		}
	}

	data := make([]byte, torusBlockSize)
	order.PutUint64(data, 0x0101010101010101)

	// Writing one more block should fail
	w := &writeOp{torus.BlockRefFromUint64s(1, 0, 0), data, true}
	err = w.perform(dbt.db)
	if err != nil {
		t.Errorf("error encountered when performing %s\nerror: %v", w.String(), err)
	}

	// Deleting one of the blocks should allow us to then write one more block
	d := &deleteOp{torus.BlockRefFromUint64s(1, 1, 1), false}
	err = d.perform(dbt.db)
	if err != nil {
		t.Errorf("error encountered when performing %s\nerror: %v", d.String(), err)
	}

	// And with that block deleted, we should be able to successfully write one
	// more block
	w.errExpected = false
	err = w.perform(dbt.db)
	if err != nil {
		t.Errorf("error encountered when performing %s\nerror: %v", w.String(), err)
	}
}

func TestBlockDeviceBlockIterator(t *testing.T) {
	dbt, err := newDeviceBlockTester(1024 * 1024 * 100) // 100 MiB
	if err != nil {
		t.Fatalf("newDeviceBlockTester: %v\n", err)
	}
	defer func() {
		err = dbt.Close(t)
		if err != nil {
			t.Fatalf("dbt.Close(): %v\n", err)
		}
	}()
	bi := dbt.db.BlockIterator()

	// The device should be empty now
	val := bi.BlockRef()
	if !val.IsZero() {
		t.Errorf("block iterator returned value when it shouldn't have: %q", val.String())
	}
	hasNext := bi.Next()
	if hasNext {
		t.Errorf("block iterator advanced when it shouldn't have")
	}
	err = bi.Err()
	if err != nil {
		t.Errorf("block iterator errored when it shouldn't have: %v", err)
	}
	err = bi.Close()
	if err != nil {
		t.Errorf("block iterator errored on closing when it shouldn't have: %v", err)
	}

	// Let's fill up the device
	order := binary.LittleEndian
	var i uint64
	for i = 1; i <= dbt.db.NumBlocks(); i++ {
		buf := make([]byte, torusBlockSize)
		order.PutUint64(buf[0:8], i)
		w := &writeOp{torus.BlockRefFromUint64s(i, i, i), buf, false}
		err := w.perform(dbt.db)
		if err != nil {
			t.Errorf("error encountered when performing %s\nerror: %v", w.String(), err)
		}
	}

	// Now let's iterate over all the blocks again, checking that we see all of
	// them
	blocksSeen := make([]bool, dbt.db.NumBlocks())
	bi = dbt.db.BlockIterator()
	for bi.Next() {
		if err := bi.Err(); err != nil {
			t.Errorf("block iterator errored when it shouldn't have: %v", err)
			break
		}
		br := bi.BlockRef()
		index := int(br.Index) - 1 // the block refs were 1 indexed, hence the -1
		if blocksSeen[index] {
			t.Errorf("block iterator returned this block ref twice: %q", br.String())
		}
		blocksSeen[index] = true
	}
	if err := bi.Close(); err != nil {
		t.Errorf("block iterator errored on closing when it shouldn't have: %v", err)
	}

	// Have we seen all of the block refs?
	for i, val := range blocksSeen {
		if !val {
			t.Errorf("we never saw block ref %x", i+1) // Again, the block refs are 1 indexed
		}
	}
}
