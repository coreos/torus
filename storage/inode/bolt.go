package inode

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"golang.org/x/net/context"

	"github.com/boltdb/bolt"
	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

var _ agro.INodeStore = &boltINodeStore{}

func init() {
	agro.RegisterINodeStore("bolt", openBoltINodeStore)
}

type boltINodeStore struct {
	db       *bolt.DB
	filename string
}

func openBoltINodeStore(name string, cfg agro.Config) (agro.INodeStore, error) {
	filename := fmt.Sprintf("inodes-%s.bolt", name)
	boltdata := filepath.Join(cfg.DataDir, "inode", filename)
	db, err := bolt.Open(boltdata, 0600, nil)
	if err != nil {
		return nil, err
	}
	return &boltINodeStore{
		db:       db,
		filename: boltdata,
	}, nil
}

func (b *boltINodeStore) ReplaceINodeStore(is agro.INodeStore) (agro.INodeStore, error) {
	new, ok := is.(*boltINodeStore)
	if !ok {
		return nil, errors.New("not replacing a bolt inode store")
	}
	err := os.Remove(b.filename)
	if err != nil {
		return nil, err
	}
	err = os.Rename(new.filename, b.filename)
	if err != nil {
		return nil, err
	}
	err = b.db.Close()
	if err != nil {
		return nil, err
	}
	out := &boltINodeStore{
		db:       new.db,
		filename: b.filename,
	}
	new.db = nil
	return out, nil
}

func (b *boltINodeStore) Flush() error {
	return b.db.Sync()
}

func (b *boltINodeStore) Close() error {
	return b.db.Close()
}

func (b *boltINodeStore) GetINode(_ context.Context, i agro.INodeRef) (*models.INode, error) {
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
	key := strconv.FormatUint(uint64(i.INode), 10)
	vol := strconv.FormatUint(uint64(i.Volume), 10)
	return key, vol
}

func (b *boltINodeStore) WriteINode(_ context.Context, i agro.INodeRef, inode *models.INode) error {
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

func (b *boltINodeStore) DeleteINode(_ context.Context, i agro.INodeRef) error {
	key, vol := formatKeyVol(i)
	err := b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(vol))
		return b.Delete([]byte(key))
	})
	return err
}

func (b *boltINodeStore) INodeIterator() agro.INodeIterator {
	tx, err := b.db.Begin(false)
	return &boltINodeIterator{
		tx:  tx,
		err: err,
	}
}

type boltINodeIterator struct {
	tx           *bolt.Tx
	err          error
	cursor       *bolt.Cursor
	curBucket    []byte
	bucketCursor *bolt.Cursor
	result       []byte
}

func (b *boltINodeIterator) Err() error {
	return b.err
}

func (b *boltINodeIterator) Next() bool {
	if b.err != nil || b.tx == nil {
		return false
	}
	if b.bucketCursor == nil {
		b.bucketCursor = b.tx.Cursor()
		b.curBucket, _ = b.bucketCursor.First()
	}
	if b.curBucket == nil {
		return false
	}
	if b.cursor == nil {
		b.cursor = b.tx.Bucket(b.curBucket).Cursor()
		b.result, _ = b.cursor.First()
	} else {
		b.result, _ = b.cursor.Next()
	}
	if b.result == nil {
		b.curBucket, _ = b.bucketCursor.Next()
		b.cursor = nil
		return b.Next()
	}
	return true
}

func (b *boltINodeIterator) INodeRef() agro.INodeRef {
	vol, err := strconv.ParseUint(string(b.curBucket), 10, 64)
	if err != nil {
		panic(err)
	}
	inode, err := strconv.ParseUint(string(b.result), 10, 64)
	if err != nil {
		panic(err)
	}
	return agro.INodeRef{
		Volume: agro.VolumeID(vol),
		INode:  agro.INodeID(inode),
	}
}

func (b *boltINodeIterator) Close() error {
	tx := b.tx
	b.tx = nil
	return tx.Rollback()
}
