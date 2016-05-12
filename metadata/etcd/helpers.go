package etcd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path"
)

func MkKey(s ...string) string {
	s = append([]string{KeyPrefix}, s...)
	return path.Join(s...)
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
