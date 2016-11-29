package blockset

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strconv"

	"golang.org/x/net/context"

	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/torus"
)

type replicationBlockset struct {
	rep       int
	sub       blockset
	repBlocks [][]torus.BlockRef
	bs        torus.BlockStore
}

var _ blockset = &replicationBlockset{}

const defaultReplication = 2

func init() {
	RegisterBlockset(Replication, func(opt string, bs torus.BlockStore, sub blockset) (blockset, error) {
		if opt == "" {
			return newReplicationBlockset(sub, bs, defaultReplication), nil
		}
		r, err := strconv.Atoi(opt)
		if err != nil {
			clog.Errorf("unknown replication amount %s: %v", opt, err)
			return nil, errors.New("unknown replication amount: " + opt)
		}
		return newReplicationBlockset(sub, bs, r), nil
	})
}

func newReplicationBlockset(sub blockset, bs torus.BlockStore, r int) *replicationBlockset {
	return &replicationBlockset{
		rep:       r,
		sub:       sub,
		bs:        bs,
		repBlocks: make([][]torus.BlockRef, r-1),
	}
}

func (b *replicationBlockset) Length() int {
	return b.sub.Length()
}

func (b *replicationBlockset) Kind() uint32 {
	return uint32(Replication)
}

func (b *replicationBlockset) GetBlock(ctx context.Context, i int) ([]byte, error) {
	if b.rep == 0 {
		return nil, torus.ErrBlockUnavailable
	}
	newctx := context.WithValue(ctx, "replication", b.rep)
	bytes, err := b.sub.GetBlock(newctx, i)
	if err == nil || err == torus.ErrBlockNotExist {
		return bytes, err
	}
	for rep := 0; rep < (b.rep - 1); rep++ {
		bytes, err := b.bs.GetBlock(ctx, b.repBlocks[rep][i])
		if err == nil {
			return bytes, nil
		}
	}
	return nil, torus.ErrBlockUnavailable
}

func (b *replicationBlockset) PutBlock(ctx context.Context, inode torus.INodeRef, i int, data []byte) error {
	if b.rep == 0 {
		return torus.ErrBlockUnavailable
	}
	err := b.sub.PutBlock(ctx, inode, i, data)
	if err != nil {
		return err
	}
	for rep := 0; rep < (b.rep - 1); rep++ {
		newBlockID := b.makeID(inode)
		err := b.bs.WriteBlock(ctx, newBlockID, data)
		if err != nil {
			return err
		}
		if i == len(b.repBlocks[rep]) {
			b.repBlocks[rep] = append(b.repBlocks[rep], newBlockID)
		} else {
			b.repBlocks[rep][i] = newBlockID
		}
	}
	return nil
}

func (b *replicationBlockset) makeID(i torus.INodeRef) torus.BlockRef {
	return b.sub.makeID(i)
}

func (b *replicationBlockset) setStore(s torus.BlockStore) {
	b.bs = s
	b.sub.setStore(s)
}

func (b *replicationBlockset) getStore() torus.BlockStore {
	return b.bs
}

func (b *replicationBlockset) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, int32(b.rep))
	if err != nil {
		return nil, err
	}
	for rep := 0; rep < (b.rep - 1); rep++ {
		err := binary.Write(buf, binary.LittleEndian, int32(len(b.repBlocks[rep])))
		if err != nil {
			return nil, err
		}
		for _, x := range b.repBlocks[rep] {
			_, err := buf.Write(x.ToBytes())
			if err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

func (b *replicationBlockset) Unmarshal(data []byte) error {
	r := bytes.NewReader(data)
	var rep int32
	err := binary.Read(r, binary.LittleEndian, &rep)
	if err != nil {
		return err
	}
	b.rep = int(rep)
	b.repBlocks = make([][]torus.BlockRef, rep)
	for rep := 0; rep < (b.rep - 1); rep++ {
		var l int32
		err := binary.Read(r, binary.LittleEndian, &l)
		if err != nil {
			return err
		}
		out := make([]torus.BlockRef, l)
		for i := 0; i < int(l); i++ {
			buf := make([]byte, torus.BlockRefByteSize)
			_, err := r.Read(buf)
			if err != nil {
				return err
			}
			out[i] = torus.BlockRefFromBytes(buf)
		}
		b.repBlocks[rep] = out
	}
	return nil
}

func (b *replicationBlockset) GetSubBlockset() torus.Blockset { return b.sub }

func (b *replicationBlockset) GetLiveINodes() *roaring.Bitmap {
	return b.sub.GetLiveINodes()
}

func (b *replicationBlockset) Truncate(lastIndex int, blocksize uint64) error {
	err := b.sub.Truncate(lastIndex, blocksize)
	if err != nil {
		return err
	}
	if lastIndex <= len(b.repBlocks[0]) {
		for i := range b.repBlocks {
			b.repBlocks[i] = b.repBlocks[i][:lastIndex]
		}
		return nil
	}
	for i := range b.repBlocks {
		toadd := lastIndex - len(b.repBlocks[0])
		for toadd != 0 {
			b.repBlocks[i] = append(b.repBlocks[i], torus.ZeroBlock())
			toadd--
		}
	}
	return nil
}

func (b *replicationBlockset) Trim(from, to int) error {
	err := b.sub.Trim(from, to)
	if err != nil {
		return err
	}
	if from >= len(b.repBlocks[0]) {
		return nil
	}
	if to > len(b.repBlocks[0]) {
		to = len(b.repBlocks[0])
	}
	for i := from; i < to; i++ {
		for _, blist := range b.repBlocks {
			if len(blist) == 0 {
				continue
			}
			blist[i] = torus.ZeroBlock()
		}
	}
	return nil
}

func (b *replicationBlockset) GetAllBlockRefs() []torus.BlockRef {
	sub := b.sub.GetAllBlockRefs()
	out := make([]torus.BlockRef, len(b.repBlocks[0])*len(b.repBlocks))
	nblocks := len(b.repBlocks[0])
	for i, list := range b.repBlocks {
		copy(out[i*nblocks:], list)
	}
	return append(sub, out...)
}

func (b *replicationBlockset) String() string {
	return "rep\n" + b.sub.String()
}
