package etcd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path"

	"github.com/RoaringBitmap/roaring"
	etcdpb "github.com/coreos/agro/internal/etcdproto/etcdserverpb"
	"github.com/coreos/agro/models"
)

func mkKey(s ...string) []byte {
	s = append([]string{keyPrefix}, s...)
	return []byte(path.Join(s...))
}

func uint64ToBytes(x uint64) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func roaringToBytes(r *roaring.Bitmap) []byte {
	buf := new(bytes.Buffer)
	_, err := r.WriteTo(buf)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func bytesToRoaring(b []byte) *roaring.Bitmap {
	r := bytes.NewReader(b)
	bm := roaring.NewBitmap()
	_, err := bm.ReadFrom(r)
	if err != nil {
		panic(err)
	}
	return bm
}

func bytesToUint64(b []byte) uint64 {
	r := bytes.NewReader(b)
	var out uint64
	err := binary.Read(r, binary.LittleEndian, &out)
	if err != nil {
		panic(err)
	}
	return out
}

func uint64ToHex(x uint64) string {
	return fmt.Sprintf("%x", x)
}

type transact struct {
	tx *etcdpb.TxnRequest
}

func tx() *transact {
	t := transact{&etcdpb.TxnRequest{}}
	return &t
}

func (t *transact) If(comps ...*etcdpb.Compare) *transact {
	t.tx.Compare = comps
	return t
}

func requestUnion(comps ...interface{}) []*etcdpb.RequestUnion {
	var out []*etcdpb.RequestUnion
	for _, v := range comps {
		switch m := v.(type) {
		case *etcdpb.RangeRequest:
			out = append(out, &etcdpb.RequestUnion{&etcdpb.RequestUnion_RequestRange{RequestRange: m}})
		case *etcdpb.PutRequest:
			out = append(out, &etcdpb.RequestUnion{&etcdpb.RequestUnion_RequestPut{RequestPut: m}})
		case *etcdpb.DeleteRangeRequest:
			out = append(out, &etcdpb.RequestUnion{&etcdpb.RequestUnion_RequestDeleteRange{RequestDeleteRange: m}})
		default:
			panic("cannot create this request option within a requestUnion")
		}
	}
	return out
}

func (t *transact) Do(comps ...interface{}) *transact {
	return t.Then(comps...)
}

func (t *transact) Then(comps ...interface{}) *transact {
	ru := requestUnion(comps...)
	t.tx.Success = ru
	return t
}

func (t *transact) Else(comps ...interface{}) *transact {
	ru := requestUnion(comps...)
	t.tx.Failure = ru
	return t
}

func (t *transact) Tx() *etcdpb.TxnRequest {
	return t.tx
}

func keyEquals(key []byte, value []byte) *etcdpb.Compare {
	return &etcdpb.Compare{
		Target: etcdpb.Compare_VALUE,
		Key:    key,
		Result: etcdpb.Compare_EQUAL,
		TargetUnion: &etcdpb.Compare_Value{
			Value: value,
		},
	}
}

func keyExists(key []byte) *etcdpb.Compare {
	return &etcdpb.Compare{
		Target: etcdpb.Compare_VERSION,
		Result: etcdpb.Compare_GREATER,
		Key:    key,
		TargetUnion: &etcdpb.Compare_Version{
			Version: 0,
		},
	}
}

func keyNotExists(key []byte) *etcdpb.Compare {
	return &etcdpb.Compare{
		Target: etcdpb.Compare_VERSION,
		Result: etcdpb.Compare_LESS,
		Key:    key,
		TargetUnion: &etcdpb.Compare_Version{
			Version: 1,
		},
	}
}

func keyIsVersion(key []byte, version int64) *etcdpb.Compare {
	return &etcdpb.Compare{
		Target: etcdpb.Compare_VERSION,
		Result: etcdpb.Compare_EQUAL,
		Key:    key,
		TargetUnion: &etcdpb.Compare_Version{
			Version: version,
		},
	}
}

func setKey(key []byte, value []byte) *etcdpb.PutRequest {
	return &etcdpb.PutRequest{
		Key:   key,
		Value: value,
	}
}

func setLeasedKey(lease int64, key []byte, value []byte) *etcdpb.PutRequest {
	return &etcdpb.PutRequest{
		Key:   key,
		Value: value,
		Lease: lease,
	}
}

func deleteKey(key []byte) *etcdpb.DeleteRangeRequest {
	return &etcdpb.DeleteRangeRequest{
		Key: key,
	}
}

func getKey(key []byte) *etcdpb.RangeRequest {
	return &etcdpb.RangeRequest{
		Key: key,
	}
}

func getPrefix(key []byte) *etcdpb.RangeRequest {
	end := make([]byte, len(key))
	copy(end, key)
	end[len(end)-1]++
	return &etcdpb.RangeRequest{
		Key:      key,
		RangeEnd: end,
	}
}

// *********

func newDirProto(md *models.Metadata) []byte {
	a := models.Directory{
		Metadata: md,
		Files:    make(map[string]*models.FileEntry),
	}
	b, err := a.Marshal()
	if err != nil {
		panic(err)
	}
	return b
}
