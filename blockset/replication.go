package blockset

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strconv"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
	"github.com/tgruben/roaring"
)

type replicationBlockset struct {
	rep       int
	sub       blockset
	repBlocks [][]agro.BlockRef
	bs        agro.BlockStore
}

var _ blockset = &replicationBlockset{}

func init() {
	RegisterBlockset(Replication, func(opt string, bs agro.BlockStore, sub blockset) (blockset, error) {
		if opt == "" {
			clog.Debugf("using default replication (0) to unmarshal")
			return newReplicationBlockset(sub, bs, 0), nil
		}
		r, err := strconv.Atoi(opt)
		if err != nil {
			clog.Errorf("unknown replication amount %s", opt)
			return nil, errors.New("unknown replication amount: " + opt)
		}
		return newReplicationBlockset(sub, bs, r), nil
	})
}

func newReplicationBlockset(sub blockset, bs agro.BlockStore, r int) *replicationBlockset {
	return &replicationBlockset{
		rep:       r,
		sub:       sub,
		bs:        bs,
		repBlocks: make([][]agro.BlockRef, r-1),
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
		return nil, agro.ErrBlockUnavailable
	}
	newctx := context.WithValue(ctx, "replication", b.rep)
	bytes, err := b.sub.GetBlock(newctx, i)
	if err == nil || err == agro.ErrBlockNotExist {
		return bytes, err
	}
	for rep := 0; rep < (b.rep - 1); rep++ {
		bytes, err := b.bs.GetBlock(ctx, b.repBlocks[rep][i])
		if err == nil {
			return bytes, nil
		}
	}
	return nil, agro.ErrBlockUnavailable
}

func (b *replicationBlockset) PutBlock(ctx context.Context, inode agro.INodeRef, i int, data []byte) error {
	if b.rep == 0 {
		return agro.ErrBlockUnavailable
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

func (b *replicationBlockset) makeID(i agro.INodeRef) agro.BlockRef {
	return b.sub.makeID(i)
}

func (b *replicationBlockset) setStore(s agro.BlockStore) {
	b.bs = s
	b.sub.setStore(s)
}

func (b *replicationBlockset) getStore() agro.BlockStore {
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
	b.repBlocks = make([][]agro.BlockRef, rep)
	for rep := 0; rep < (b.rep - 1); rep++ {
		var l int32
		err := binary.Read(r, binary.LittleEndian, &l)
		if err != nil {
			return err
		}
		out := make([]agro.BlockRef, l)
		for i := 0; i < int(l); i++ {
			buf := make([]byte, agro.BlockRefByteSize)
			_, err := r.Read(buf)
			if err != nil {
				return err
			}
			out[i] = agro.BlockRefFromBytes(buf)
		}
		b.repBlocks[rep] = out
	}
	return nil
}

func (b *replicationBlockset) GetSubBlockset() agro.Blockset { return b.sub }

func (b *replicationBlockset) GetLiveINodes() *roaring.RoaringBitmap {
	return b.sub.GetLiveINodes()
}

func (b *replicationBlockset) Truncate(lastIndex int) error {
	err := b.sub.Truncate(lastIndex)
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
			b.repBlocks[i] = append(b.repBlocks[i], agro.ZeroBlock())
			toadd--
		}
	}
	return nil
}
