package etcd

import (
	"bytes"
	"encoding/binary"
	"path"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
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

func uint64FromBytes(b []byte) uint64 {
	r := bytes.NewReader(b)
	var out uint64
	err := binary.Read(r, binary.LittleEndian, &out)
	if err != nil {
		panic(err)
	}
	return out
}

type transact pb.TxnRequest

func tx() *transact {
	t := transact(pb.TxnRequest{})
	return &t
}

func (t *transact) If(comps ...*pb.Compare) {
	t.Compare = comps
}

func requestUnion(comps ...interface{}) []*pb.RequestUnion {
	var out []*pb.RequestUnion
	for _, v := range comps {
		switch m := v.(type) {
		case *pb.RangeRequest:
			out = append(out, &pb.RequestUnion{RequestRange: m})
		case *pb.PutRequest:
			out = append(out, &pb.RequestUnion{RequestPut: m})
		case *pb.DeleteRangeRequest:
			out = append(out, &pb.RequestUnion{RequestDeleteRange: m})
		default:
			panic("cannot create this request option within a requestUnion")
		}
	}
	return out
}

func (t *transact) txThen(comps ...interface{}) *transact {
	ru := requestUnion(comps...)
	t.Success = ru
	return t
}

func (t *transact) Else(comps ...interface{}) *transact {
	ru := requestUnion(comps...)
	t.Failure = ru
	return t
}

func keyEquals(key []byte, value []byte) *pb.Compare {
	return &pb.Compare{
		Target: pb.Compare_VALUE,
		Key:    key,
		Value:  value,
		Result: pb.Compare_EQUAL,
	}
}

func setKey(key []byte, value []byte) *pb.PutRequest {
	return &pb.PutRequest{
		Key:   key,
		Value: value,
	}
}

func getKey(key []byte) *pb.RangeRequest {
	return &pb.RangeRequest{
		Key: key,
	}
}

func getPrefix(key []byte) *pb.RangeRequest {
	end := make([]byte, len(key))
	copy(end, key)
	end[len(end)-1]++
	return &pb.RangeRequest{
		Key:      key,
		RangeEnd: end,
	}
}
