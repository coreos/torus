package inode

import (
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/models"
	"github.com/boltdb/bolt"
)

type boltInodeStore struct {
	db *bolt.DB
}

func OpenBoltInodeStore(cfg agro.Config) (agro.INodeStore, error) {
	boltdata := filepath.Join(cfg.DataDir, "inode", "inodes.bolt")
	db, err := bolt.Open(boltdata, 0600, nil)
	if err != nil {
		return nil, err
	}
	return &boltInodeStore{
		db: db,
	}, nil
}

func (b *boltInodeStore) Flush() error {
	return b.db.Sync()
}

func (b *boltInodeStore) Close() error {
	return b.db.Close()
}

func (b *boltInodeStore) GetINode(i agro.INodeRef) (*models.INode, error) {
	var inodeBytes []byte
	key, vol := formatKeyVol(i)
	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(vol))
		inodeBytes = b.Get([]byte(key))
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := &models.INode{}
	err = out.Unmarshal(inodeBytes)
	return out, err
}

func formatKeyVol(i agro.INodeRef) (string, string) {
	key := fmt.Sprintf("%016x", i.INode)
	vol := strconv.FormatUint(uint64(i.Volume), 10)
	return key, vol
}

func (b *boltInodeStore) WriteINode(i agro.INodeRef, inode *models.INode) error {
	inodeBytes, err := inode.Marshal()
	if err != nil {
		return err
	}
	key, vol := formatKeyVol(i)
	err = b.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(vol))
		if err != nil {
			return err
		}
		return b.Put([]byte(key), inodeBytes)
	})
	return err
}

func (b *boltInodeStore) DeleteINode(i agro.INodeRef) error {
	key, vol := formatKeyVol(i)
	err := b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(vol))
		return b.Delete([]byte(key))
	})
	return err
}
