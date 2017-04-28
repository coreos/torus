package etcd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path"
)

//this the an util file to do same basic jobs, such as:
//MkKey(),Uint64ToBytes(),BytesToUint64(),Uint64ToHex()

//combine the strings with a prefix string
//e.g. it gets "/github.com/freesky/edward" when calling
//MkKey("freesky","edward") while KeyPrefix="/github.com"
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
