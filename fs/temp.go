package fs

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/coreos/agro/models"
	"github.com/tgruben/roaring"
)

func (t *Client) Mkdir(p agro.Path, md *models.Metadata) error {
	if p.Path == "/" {
		return errors.New("can't create the root directory")
	}

	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	tx := t.srv.tree.Txn()

	k := []byte(p.Key())
	if _, ok := tx.Get(k); ok {
		return &os.PathError{
			Op:   "mkdir",
			Path: p.Path,
			Err:  os.ErrExist,
		}
	}
	tx.Insert(k, &models.Directory{
		Metadata: md,
		Files:    make(map[string]*models.FileEntry),
	})

	for {
		p.Path, _ = path.Split(strings.TrimSuffix(p.Path, "/"))
		if p.Path == "" {
			break
		}
		k = []byte(p.Key())
		if _, ok := tx.Get(k); !ok {
			return &os.PathError{
				Op:   "stat",
				Path: p.Path,
				Err:  os.ErrNotExist,
			}
		}
	}

	t.srv.tree = tx.Commit()
	return nil
}

func (t *Client) ChangeDirMetadata(p agro.Path, md *models.Metadata) error {
	if !p.IsDir() {
		return agro.ErrNotDir
	}

	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	tx := t.srv.tree.Txn()

	k := []byte(p.Key())
	v, ok := tx.Get(k)
	if !ok {
		return agro.ErrNotExist
	}
	dir := v.(*models.Directory)
	dir.Metadata = md
	tx.Insert(k, dir)
	t.srv.tree = tx.Commit()
	return nil
}

func (t *Client) Rmdir(p agro.Path) error {
	if p.Path == "/" {
		return errors.New("can't delete the root directory")
	}
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	tx := t.srv.tree.Txn()

	k := []byte(p.Key())
	v, ok := tx.Get(k)
	if !ok {
		return &os.PathError{
			Op:   "rmdir",
			Path: p.Path,
			Err:  os.ErrNotExist,
		}
	}
	dir := v.(*models.Directory)
	if len(dir.Files) != 0 {
		return &os.PathError{
			Op:   "rmdir",
			Path: p.Path,
			Err:  os.ErrInvalid,
		}
	}
	tx.Delete(k)
	t.srv.tree = tx.Commit()
	return nil
}

func (t *Client) debugPrintTree() {
	it := t.srv.tree.Root().Iterator()
	for {
		k, v, ok := it.Next()
		if !ok {
			break
		}
		fmt.Println(string(k), v)
	}
}

func (t *Client) SetFileEntry(p agro.Path, ent *models.FileEntry) error {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	var (
		tx = t.srv.tree.Txn()
		k  = []byte(p.Key())
	)
	v, ok := tx.Get(k)
	if !ok {
		return &os.PathError{
			Op:   "stat",
			Path: p.Path,
			Err:  os.ErrNotExist,
		}
	}
	dir := v.(*models.Directory)
	if dir == nil {
		dir = &models.Directory{}
	}
	if dir.Files == nil {
		dir.Files = make(map[string]*models.FileEntry)
	}
	if ent.Chain == 0 && ent.Sympath == "" {
		delete(dir.Files, p.Filename())
	} else {
		dir.Files[p.Filename()] = ent
	}
	tx.Insert(k, dir)
	t.srv.tree = tx.Commit()
	return nil
}

func (t *Client) GetChainINode(base agro.INodeRef) (agro.INodeRef, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	return t.srv.chains[base.Volume()][base], nil
}

func (t *Client) SetChainINode(base agro.INodeRef, was agro.INodeRef, new agro.INodeRef) error {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	cur := t.srv.chains[base.Volume()][base]
	if cur.INode != was.INode {
		return agro.ErrCompareFailed
	}
	if new.INode != 0 {
		t.srv.chains[base.Volume()][base] = new
	} else {
		delete(t.srv.chains[base.Volume()], base)
	}
	return nil
}

func (t *Client) Getdir(p agro.Path) (*models.Directory, []agro.Path, error) {
	var (
		tx = t.srv.tree.Txn()
		k  = []byte(p.Key())
	)
	v, ok := tx.Get(k)
	if !ok {
		return nil, nil, &os.PathError{
			Op:   "stat",
			Path: p.Path,
			Err:  os.ErrNotExist,
		}
	}

	var (
		dir     = v.(*models.Directory)
		prefix  = []byte(p.SubdirsPrefix())
		subdirs []agro.Path
	)
	tx.Root().WalkPrefix(prefix, func(k []byte, v interface{}) bool {
		subdirs = append(subdirs, agro.Path{
			Volume: p.Volume,
			Path:   fmt.Sprintf("%s%s", p.Path, bytes.TrimPrefix(k, prefix)),
		})
		return false
	})
	return dir, subdirs, nil
}

func (t *Client) ClaimVolumeINodes(_ int64, vol agro.VolumeID, inodes *roaring.Bitmap) error {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	t.srv.openINodes[t.uuid][vol] = inodes
	return nil
}

func (t *Client) ModifyDeadMap(vol agro.VolumeID, live *roaring.Bitmap, dead *roaring.Bitmap) error {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	x, ok := t.srv.deadMap[vol]
	if !ok {
		x = roaring.NewBitmap()
	}
	x.Or(dead)
	x.AndNot(live)
	t.srv.deadMap[vol] = x
	return nil
}

func (t *Client) GetVolumeLiveness(vol agro.VolumeID) (*roaring.Bitmap, []*roaring.Bitmap, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	x, ok := t.srv.deadMap[vol]
	if !ok {
		x = roaring.NewBitmap()
	}
	var l []*roaring.Bitmap
	for _, perclient := range t.srv.openINodes {
		if c, ok := perclient[vol]; ok {
			if c != nil {
				l = append(l, c)
			}
		}
	}
	return x, l, nil
}

func (t *Client) GetINodeChains(vid agro.VolumeID) ([]*models.FileChainSet, error) {
	if chain, ok := t.srv.chains[vid]; ok {
		fcs := &models.FileChainSet{
			Chains: make(map[uint64]uint64),
		}
		for k, v := range chain {
			fcs.Chains[uint64(k.INode)] = uint64(v.INode)
		}
		return []*models.FileChainSet{fcs}, nil
	}
	return nil, errors.New("invalid volume ID")
}
