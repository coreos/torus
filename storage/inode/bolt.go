package inode

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

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
	mut      sync.RWMutex
	db       *bolt.DB
	filename string
	name     string
}

func openBoltINodeStore(name string, cfg agro.Config) (agro.INodeStore, error) {
	filename := fmt.Sprintf("inodes-%s.bolt", name)
	boltdata := filepath.Join(cfg.DataDir, "inode", filename)
	db, err := bolt.Open(boltdata, 0600, nil)
	if err != nil {
		return nil, err
	}
	b := &boltINodeStore{
		db:       db,
		filename: boltdata,
		name:     name,
	}
	b.updateNumINodes()
	return b, nil
}

func (b *boltINodeStore) updateNumINodes() {
	b.mut.RLock()
	defer b.mut.RUnlock()
	out := 0
	b.db.View(func(tx *bolt.Tx) error {
		tx.ForEach(func(_ []byte, b *bolt.Bucket) error {
			out += b.Stats().KeyN
			return nil
		})
		return nil
	})
	promINodes.WithLabelValues(b.name).Set(float64(out))
}

func (b *boltINodeStore) ReplaceINodeStore(is agro.INodeStore) (agro.INodeStore, error) {
	newBolt, ok := is.(*boltINodeStore)
	if !ok {
		return nil, errors.New("not replacing a bolt inode store")
	}
	b.mut.Lock()
	defer b.mut.Unlock()
	newBolt.mut.Lock()
	defer newBolt.mut.Unlock()
	err := os.Remove(b.filename)
	if err != nil {
		return nil, err
	}
	err = os.Rename(newBolt.filename, b.filename)
	if err != nil {
		return nil, err
	}
	err = b.db.Close()
	if err != nil {
		return nil, err
	}
	out := &boltINodeStore{
		db:       newBolt.db,
		filename: b.filename,
		name:     b.name,
	}
	newBolt.db = nil
	out.updateNumINodes()
	return out, nil
}

func (b *boltINodeStore) Flush() error {
	b.mut.Lock()
	defer b.mut.Unlock()
	promINodeFlushes.WithLabelValues(b.name).Inc()
	return b.db.Sync()
}

func (b *boltINodeStore) Close() error {
	b.mut.Lock()
	defer b.mut.Unlock()
	return b.db.Close()
}

func (b *boltINodeStore) Kind() string {
	return "bolt"
}

func (b *boltINodeStore) GetINode(_ context.Context, i agro.INodeRef) (*models.INode, error) {
	b.mut.RLock()
	defer b.mut.RUnlock()
	var inodeBytes []byte
	key, vol := formatKeyVol(i)
	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(vol))
		inodeBytes = b.Get([]byte(key))
		return nil
	})
	if err != nil {
		promINodesFailed.WithLabelValues(b.name).Inc()
		return nil, err
	}
	out := &models.INode{}
	err = out.Unmarshal(inodeBytes)
	promINodesRetrieved.WithLabelValues(b.name).Inc()
	return out, err
}

func formatKeyVol(i agro.INodeRef) (string, string) {
	key := strconv.FormatUint(uint64(i.INode), 10)
	vol := strconv.FormatUint(uint64(i.Volume()), 10)
	return key, vol
}

func (b *boltINodeStore) WriteINode(_ context.Context, i agro.INodeRef, inode *models.INode) error {
	b.mut.Lock()
	defer b.mut.Unlock()
	inodeBytes, err := inode.Marshal()
	if err != nil {
		promINodeWritesFailed.WithLabelValues(b.name).Inc()
		return err
	}
	key, vol := formatKeyVol(i)
	err = b.db.Update(func(tx *bolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte(vol))
		if err != nil {
			return err
		}
		return bk.Put([]byte(key), inodeBytes)
	})
	if err != nil {
		promINodeWritesFailed.WithLabelValues(b.name).Inc()
		return err
	}
	promINodesWritten.WithLabelValues(b.name).Inc()
	promINodes.WithLabelValues(b.name).Inc()
	return nil
}

func (b *boltINodeStore) DeleteINode(_ context.Context, i agro.INodeRef) error {
	b.mut.Lock()
	defer b.mut.Unlock()
	key, vol := formatKeyVol(i)
	err := b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(vol))
		return b.Delete([]byte(key))
	})
	if err != nil {
		promINodeDeletesFailed.WithLabelValues(b.name).Inc()
		return err
	}
	promINodesDeleted.WithLabelValues(b.name).Inc()
	promINodes.WithLabelValues(b.name).Dec()
	return nil
}

func (b *boltINodeStore) INodeIterator() agro.INodeIterator {
	b.mut.RLock()
	defer b.mut.RUnlock()
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
	return agro.NewINodeRef(
		agro.VolumeID(vol),
		agro.INodeID(inode),
	)
}

func (b *boltINodeIterator) Close() error {
	tx := b.tx
	b.tx = nil
	return tx.Rollback()
}
