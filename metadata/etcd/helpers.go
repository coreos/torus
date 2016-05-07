package etcd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path"

	etcdpb "github.com/coreos/agro/internal/etcdproto/etcdserverpb"
)

func MkKey(s ...string) []byte {
	s = append([]string{KeyPrefix}, s...)
	return []byte(path.Join(s...))
}

func Uint64ToBytes(x uint64) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func BytesToUint64(b []byte) uint64 {
	r := bytes.NewReader(b)
	var out uint64
	err := binary.Read(r, binary.LittleEndian, &out)
	if err != nil {
		panic(err)
	}
	return out
}

func Uint64ToHex(x uint64) string {
	return fmt.Sprintf("%x", x)
}

type Transact struct {
	tx *etcdpb.TxnRequest
}

func Tx() *Transact {
	t := Transact{&etcdpb.TxnRequest{}}
	return &t
}

func (t *Transact) If(comps ...*etcdpb.Compare) *Transact {
	t.tx.Compare = comps
	return t
}

func requestUnion(comps ...interface{}) []*etcdpb.RequestUnion {
	var out []*etcdpb.RequestUnion
	for _, v := range comps {
		switch m := v.(type) {
		case *etcdpb.RangeRequest:
			out = append(out, &etcdpb.RequestUnion{Request: &etcdpb.RequestUnion_RequestRange{RequestRange: m}})
		case *etcdpb.PutRequest:
			out = append(out, &etcdpb.RequestUnion{Request: &etcdpb.RequestUnion_RequestPut{RequestPut: m}})
		case *etcdpb.DeleteRangeRequest:
			out = append(out, &etcdpb.RequestUnion{Request: &etcdpb.RequestUnion_RequestDeleteRange{RequestDeleteRange: m}})
		default:
			panic("cannot create this request option within a requestUnion")
		}
	}
	return out
}

func (t *Transact) Do(comps ...interface{}) *Transact {
	return t.Then(comps...)
}

func (t *Transact) Then(comps ...interface{}) *Transact {
	ru := requestUnion(comps...)
	t.tx.Success = ru
	return t
}

func (t *Transact) Else(comps ...interface{}) *Transact {
	ru := requestUnion(comps...)
	t.tx.Failure = ru
	return t
}

func (t *Transact) Tx() *etcdpb.TxnRequest {
	return t.tx
}

func KeyEquals(key []byte, value []byte) *etcdpb.Compare {
	return &etcdpb.Compare{
		Target: etcdpb.Compare_VALUE,
		Key:    key,
		Result: etcdpb.Compare_EQUAL,
		TargetUnion: &etcdpb.Compare_Value{
			Value: value,
		},
	}
}

func KeyExists(key []byte) *etcdpb.Compare {
	return &etcdpb.Compare{
		Target: etcdpb.Compare_VERSION,
		Result: etcdpb.Compare_GREATER,
		Key:    key,
		TargetUnion: &etcdpb.Compare_Version{
			Version: 0,
		},
	}
}

func KeyNotExists(key []byte) *etcdpb.Compare {
	return &etcdpb.Compare{
		Target: etcdpb.Compare_VERSION,
		Result: etcdpb.Compare_LESS,
		Key:    key,
		TargetUnion: &etcdpb.Compare_Version{
			Version: 1,
		},
	}
}

func KeyIsVersion(key []byte, version int64) *etcdpb.Compare {
	return &etcdpb.Compare{
		Target: etcdpb.Compare_VERSION,
		Result: etcdpb.Compare_EQUAL,
		Key:    key,
		TargetUnion: &etcdpb.Compare_Version{
			Version: version,
		},
	}
}

func SetKey(key []byte, value []byte) *etcdpb.PutRequest {
	return &etcdpb.PutRequest{
		Key:   key,
		Value: value,
	}
}

func SetLeasedKey(lease int64, key []byte, value []byte) *etcdpb.PutRequest {
	return &etcdpb.PutRequest{
		Key:   key,
		Value: value,
		Lease: lease,
	}
}

func DeleteKey(key []byte) *etcdpb.DeleteRangeRequest {
	return &etcdpb.DeleteRangeRequest{
		Key: key,
	}
}

func DeletePrefix(key []byte) *etcdpb.DeleteRangeRequest {
	end := make([]byte, len(key))
	copy(end, key)
	end[len(end)-1]++
	return &etcdpb.DeleteRangeRequest{
		Key:      key,
		RangeEnd: end,
	}
}

func GetKey(key []byte) *etcdpb.RangeRequest {
	return &etcdpb.RangeRequest{
		Key: key,
	}
}

func GetPrefix(key []byte) *etcdpb.RangeRequest {
	end := make([]byte, len(key))
	copy(end, key)
	end[len(end)-1]++
	return &etcdpb.RangeRequest{
		Key:      key,
		RangeEnd: end,
	}
}
