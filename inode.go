package server

import (
	"encoding/binary"

	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
)

var (
	// INodes
	promINodeRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_inode_requests_total",
		Help: "Total number of inodes requested of the distributor layer",
	})
	promINodeFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "agro_distributor_inode_request_failures",
		Help: "Number of failed inode requests",
	})
)

func init() {
	prometheus.MustRegister(promINodeRequests)
	prometheus.MustRegister(promINodeFailures)
}

type INodeStore struct {
	bs   agro.BlockStore
	name string
}

func NewINodeStore(bs agro.BlockStore) *INodeStore {
	return &INodeStore{
		bs: bs,
	}
}

func (b *INodeStore) Flush() error { return b.bs.Flush() }
func (b *INodeStore) Close() error {
	return b.bs.Close()
}

func (b *INodeStore) WriteINode(ctx context.Context, i agro.INodeRef, inode *models.INode) error {
	if i.INode == 0 {
		panic("Writing zero inode")
	}
	inodedata, err := inode.Marshal()
	if err != nil {
		return err
	}
	buf := make([]byte, b.bs.BlockSize())
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(inodedata)))
	bufoffset := 4
	inodeoffset := 0
	index := 1
	for inodeoffset != len(inodedata) {
		if bufoffset == 0 {
			buf = make([]byte, b.bs.BlockSize())
		}
		written := copy(buf[bufoffset:], inodedata[inodeoffset:])
		inodeoffset += written
		ref := agro.BlockRef{
			INodeRef: i,
			Index:    agro.IndexID(index),
		}
		ref.SetBlockType(agro.TypeINode)
		err := b.bs.WriteBlock(ctx, ref, buf)
		if err != nil {
			return err
		}
		bufoffset = 0
		index++
	}
	clog.Tracef("Wrote INode %s", i)
	return nil
}

func (b *INodeStore) GetINode(ctx context.Context, i agro.INodeRef) (*models.INode, error) {
	if i.INode == 0 {
		panic("Fetching zero inode")
	}
	promINodeRequests.Inc()
	index := 1
	ref := agro.BlockRef{
		INodeRef: i,
		Index:    agro.IndexID(index),
	}
	ref.SetBlockType(agro.TypeINode)
	data, err := b.bs.GetBlock(ctx, ref)
	if err != nil {
		promINodeFailures.Inc()
		return nil, err
	}
	dlen := binary.LittleEndian.Uint32(data[0:4])
	buf := make([]byte, dlen)
	bufoffset := 0
	dataoffset := 4
	for bufoffset != int(dlen) {
		if dataoffset == 0 {
			index++
			ref := agro.BlockRef{
				INodeRef: i,
				Index:    agro.IndexID(index),
			}
			ref.SetBlockType(agro.TypeINode)
			data, err = b.bs.GetBlock(ctx, ref)
			if err != nil {
				promINodeFailures.Inc()
				clog.Errorf("inode: couldn't get inode block: %s -- %s", err, ref)
				return nil, err
			}
		}
		written := copy(buf[bufoffset:], data[dataoffset:])
		dataoffset = 0
		bufoffset += written
	}
	out := &models.INode{}
	err = out.Unmarshal(buf)
	if err != nil {
		promINodeFailures.Inc()
		clog.Errorf("inode: couldn't unmarshal: %s", err)
		return nil, err
	}
	return out, nil
}

func (b *INodeStore) DeleteINode(ctx context.Context, i agro.INodeRef) error {
	if i.INode == 0 {
		panic("Deleting zero inode")
	}
	ref := agro.BlockRef{
		INodeRef: i,
		Index:    agro.IndexID(1),
	}
	ref.SetBlockType(agro.TypeINode)
	data, err := b.bs.GetBlock(ctx, ref)
	if err != nil {
		return err
	}
	dlen := binary.LittleEndian.Uint32(data[0:4])
	nblocks := (uint64(dlen) / b.bs.BlockSize()) + 1
	for j := uint64(1); j <= nblocks; j++ {
		ref := agro.BlockRef{
			INodeRef: i,
			Index:    agro.IndexID(j),
		}
		ref.SetBlockType(agro.TypeINode)
		err := b.bs.DeleteBlock(ctx, ref)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *INodeStore) INodeIterator() *INodeIterator {
	it := b.bs.BlockIterator()
	return &INodeIterator{it}
}

type INodeIterator struct {
	it agro.BlockIterator
}

func (i *INodeIterator) Err() error { return i.it.Err() }
func (i *INodeIterator) Next() bool {
	for i.it.Next() {
		ref := i.it.BlockRef()
		if ref.BlockType() == agro.TypeINode && ref.Index == 1 {
			return true
		}
	}
	return false
}

func (i *INodeIterator) INodeRef() agro.INodeRef {
	return i.it.BlockRef().INodeRef
}

func (i *INodeIterator) Close() error {
	return i.it.Close()
}
