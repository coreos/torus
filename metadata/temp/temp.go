package temp

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/blockset"
	"github.com/barakmich/agro/models"
	"github.com/hashicorp/go-immutable-radix"
)

func init() {
	agro.RegisterMetadataService("temp", newTempMetadata)
}

type temp struct {
	mut      sync.Mutex
	inode    agro.INodeID
	tree     *iradix.Tree
	volIndex map[string]agro.VolumeID
	vol      agro.VolumeID
	global   agro.GlobalMetadata
}

func newTempMetadata(address string) agro.MetadataService {
	return &temp{
		volIndex: make(map[string]agro.VolumeID),
		tree:     iradix.New(),
		global: agro.GlobalMetadata{
			BlockSize:        8 * 1024,
			DefaultBlockSpec: agro.BlockLayerSpec{blockset.CRC, blockset.Base},
		},
	}
}

func (t *temp) GlobalMetadata() (agro.GlobalMetadata, error) {
	return t.global, nil
}

func (t *temp) Mkfs(md agro.GlobalMetadata) error {
	t.global = md
	return nil
}

func (t *temp) CreateVolume(volume string) error {
	t.mut.Lock()
	defer t.mut.Unlock()
	tx := t.tree.Txn()

	k := []byte(agro.Path{Volume: volume, Path: "/"}.Key())
	if _, ok := tx.Get(k); !ok {
		tx.Insert(k, (*models.Directory)(nil))
		t.tree = tx.Commit()
		t.vol++
		t.volIndex[volume] = t.vol
	}

	// TODO(jzelinskie): maybe raise volume already exists
	return nil
}

func (t *temp) CommitInodeIndex() (agro.INodeID, error) {
	t.mut.Lock()
	defer t.mut.Unlock()

	t.inode++
	return t.inode, nil
}

func (t *temp) Mkdir(p agro.Path, dir *models.Directory) error {
	if p.Path == "/" {
		return errors.New("can't create the root directory")
	}

	t.mut.Lock()
	defer t.mut.Unlock()

	tx := t.tree.Txn()

	k := []byte(p.Key())
	if _, ok := tx.Get(k); ok {
		return &os.PathError{
			Op:   "mkdir",
			Path: p.Path,
			Err:  os.ErrExist,
		}
	}
	tx.Insert(k, dir)

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

	t.tree = tx.Commit()
	return nil
}

func (t *temp) debugPrintTree() {
	it := t.tree.Root().Iterator()
	for {
		k, v, ok := it.Next()
		if !ok {
			break
		}
		fmt.Println(string(k), v)
	}
}

func (t *temp) SetFileINode(p agro.Path, ref agro.INodeRef) error {
	vid, err := t.GetVolumeID(p.Volume)
	if err != nil {
		return err
	}
	if vid != ref.Volume {
		return errors.New("temp: inodeRef volume not for given path volume")
	}
	t.mut.Lock()
	defer t.mut.Unlock()
	var (
		tx = t.tree.Txn()
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
		dir.Files = make(map[string]uint64)
	}
	dir.Files[p.Filename()] = uint64(ref.INode)
	tx.Insert(k, dir)
	t.tree = tx.Commit()
	return nil
}

func (t *temp) Getdir(p agro.Path) (*models.Directory, []agro.Path, error) {
	var (
		tx = t.tree.Txn()
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

func (t *temp) GetVolumes() ([]string, error) {
	var (
		iter = t.tree.Root().Iterator()
		out  []string
		last string
	)
	for {
		k, _, ok := iter.Next()
		if !ok {
			break
		}
		if i := bytes.IndexByte(k, ':'); i != -1 {
			vol := string(k[:i])
			if vol == last {
				continue
			}
			out = append(out, vol)
			last = vol
		}
	}
	return out, nil
}

func (t *temp) GetVolumeID(volume string) (agro.VolumeID, error) {
	t.mut.Lock()
	defer t.mut.Unlock()

	if vol, ok := t.volIndex[volume]; ok {
		return vol, nil
	}
	return 0, errors.New("temp: no such volume exists")
}
