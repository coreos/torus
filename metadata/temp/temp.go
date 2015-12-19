package temp

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/blockset"
	"github.com/barakmich/agro/metadata"
	"github.com/barakmich/agro/models"
	"github.com/barakmich/agro/ring"
	"github.com/hashicorp/go-immutable-radix"
)

func init() {
	agro.RegisterMetadataService("temp", newTempMetadata)
}

type TempServer struct {
	mut sync.Mutex

	inode agro.INodeID
	vol   agro.VolumeID

	tree     *iradix.Tree
	volIndex map[string]agro.VolumeID
	global   agro.GlobalMetadata
	peers    []*models.PeerInfo
}

type TempClient struct {
	cfg  agro.Config
	uuid string
	srv  *TempServer
}

func newTempMetadata(cfg agro.Config) (agro.MetadataService, error) {
	uuid, err := metadata.MakeOrGetUUID(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	srv := &TempServer{
		volIndex: make(map[string]agro.VolumeID),
		tree:     iradix.New(),
		// TODO(barakmich): Allow creating of dynamic GMD via mkfs to the metadata directory.
		global: agro.GlobalMetadata{
			BlockSize:        8 * 1024,
			DefaultBlockSpec: blockset.MustParseBlockLayerSpec("crc,base"),
		},
	}
	return &TempClient{
		cfg:  cfg,
		uuid: uuid,
		srv:  srv,
	}, nil
}

func (t *TempClient) GlobalMetadata() (agro.GlobalMetadata, error) {
	return t.srv.global, nil
}

func (t *TempClient) UUID() string {
	return t.uuid
}

func (t *TempClient) GetPeers() ([]*models.PeerInfo, error) {
	return t.srv.peers, nil
}

func (t *TempClient) RegisterPeer(pi *models.PeerInfo) error {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	t.srv.peers = append(t.srv.peers, pi)
	return nil
}

func (t *TempClient) CreateVolume(volume string) error {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()
	if _, ok := t.srv.volIndex[volume]; ok {
		return agro.ErrExists
	}

	tx := t.srv.tree.Txn()

	k := []byte(agro.Path{Volume: volume, Path: "/"}.Key())
	if _, ok := tx.Get(k); !ok {
		tx.Insert(k, (*models.Directory)(nil))
		t.srv.tree = tx.Commit()
		t.srv.vol++
		t.srv.volIndex[volume] = t.srv.vol
	}

	// TODO(jzelinskie): maybe raise volume already exists
	return nil
}

func (t *TempClient) CommitInodeIndex() (agro.INodeID, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	t.srv.inode++
	return t.srv.inode, nil
}

func (t *TempClient) Mkdir(p agro.Path, dir *models.Directory) error {
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

	t.srv.tree = tx.Commit()
	return nil
}

func (t *TempClient) debugPrintTree() {
	it := t.srv.tree.Root().Iterator()
	for {
		k, v, ok := it.Next()
		if !ok {
			break
		}
		fmt.Println(string(k), v)
	}
}

func (t *TempClient) SetFileINode(p agro.Path, ref agro.INodeRef) error {
	vid, err := t.GetVolumeID(p.Volume)
	if err != nil {
		return err
	}
	if vid != ref.Volume {
		return errors.New("temp: inodeRef volume not for given path volume")
	}
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
		dir.Files = make(map[string]uint64)
	}
	dir.Files[p.Filename()] = uint64(ref.INode)
	tx.Insert(k, dir)
	t.srv.tree = tx.Commit()
	return nil
}

func (t *TempClient) Getdir(p agro.Path) (*models.Directory, []agro.Path, error) {
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

func (t *TempClient) GetVolumes() ([]string, error) {
	var (
		iter = t.srv.tree.Root().Iterator()
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

func (t *TempClient) GetVolumeID(volume string) (agro.VolumeID, error) {
	t.srv.mut.Lock()
	defer t.srv.mut.Unlock()

	if vol, ok := t.srv.volIndex[volume]; ok {
		return vol, nil
	}
	return 0, errors.New("temp: no such volume exists")
}

func (t *TempClient) GetRing() (agro.Ring, error) {
	return ring.CreateRing(&models.Ring{
		Type:    uint32(ring.Single),
		Version: 1,
		UUIDs:   []string{t.uuid},
	})
}

func (t *TempClient) SubscribeNewRings(ch chan agro.Ring) {
	close(ch)
}

func (t *TempClient) UnsubscribeNewRings(ch chan agro.Ring) {
	// Kay. We unsubscribed you already.
}

func (t *TempClient) Close() error { return nil }

func (t *TempClient) WithContext(_ context.Context) agro.MetadataService {
	return t
}
