package temp

import (
	"bytes"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
	"github.com/hashicorp/go-immutable-radix"
)

func init() {
	agro.RegisterMetadataProvider("temp", newTempMetadata)
}

type temp struct {
	mut   sync.Mutex
	inode uint64
	tree  *iradix.Tree
}

func newTempMetadata(address string) agro.Metadata {
	return &temp{
		tree: iradix.New(),
	}
}

func (t *temp) Mkfs() error { return nil }

func (t *temp) CreateVolume(volume string) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	tx := t.tree.Txn()

	k := []byte(agro.Path{Volume: volume, Path: "/"}.Key())
	if _, ok := tx.Get(k); !ok {
		tx.Insert(k, (*models.Directory)(nil))
		t.tree = tx.Commit()
	}
	return nil
}

func (t *temp) CommitInodeIndex() (uint64, error) {
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
		return errors.New("EEXIST: directory already exists")
	}
	tx.Insert(k, dir)

	for {
		p.Path, _ = path.Split(strings.TrimSuffix(p.Path, "/"))
		if p.Path == "" {
			break
		}
		k = []byte(p.Key())
		if _, ok := tx.Get(k); !ok {
			return fmt.Errorf("ENOENT: cannot create directory (%q) recursively", k)
		}
	}

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
		return nil, nil, errors.New("ENOENT: cannot find directory")
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
