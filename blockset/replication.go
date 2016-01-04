package blockset

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strconv"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
)

type replicationBlockset struct {
	rep int
	sub blockset
}

var _ blockset = &replicationBlockset{}

func init() {
	RegisterBlockset(Replication, func(opt string, _ agro.BlockStore, sub blockset) (blockset, error) {
		if opt == "" {
			clog.Debugf("using default replication (0) to unmarshal")
			return newReplicationBlockset(sub, 0), nil
		}
		r, err := strconv.Atoi(opt)
		if err != nil {
			clog.Errorf("unknown replication amount %s", opt)
			return nil, errors.New("unknown replication amount: " + opt)
		}
		return newReplicationBlockset(sub, r), nil
	})
}

func newReplicationBlockset(sub blockset, r int) *replicationBlockset {
	return &replicationBlockset{
		rep: r,
		sub: sub,
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
	return b.sub.GetBlock(newctx, i)
}

func (b *replicationBlockset) PutBlock(ctx context.Context, inode agro.INodeRef, i int, data []byte) error {
	if b.rep == 0 {
		return agro.ErrBlockUnavailable
	}
	newctx := context.WithValue(ctx, "replication", b.rep)
	return b.sub.PutBlock(newctx, inode, i, data)
}

func (b *replicationBlockset) makeID(i agro.INodeRef) agro.BlockRef {
	return b.sub.makeID(i)
}

func (b *replicationBlockset) setStore(s agro.BlockStore) {
	b.sub.setStore(s)
}

func (b *replicationBlockset) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, int32(b.rep))
	if err != nil {
		return nil, err
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
	return nil
}

func (b *replicationBlockset) GetSubBlockset() agro.Blockset { return b.sub }
