package agro

import (
	"errors"

	"github.com/barakmich/agro/types"
)

var ErrKeyNotFound = errors.New("Couldn't find key in storage")

type Store interface {
	Flush() error
	Close() error
}

type KeyStore interface {
	Store
	GetKey(s string) ([]byte, error)
	WriteKey(s string, data []byte) error
	DeleteKey(s string) error
}

type INodeStore interface {
	Store
	GetINode(i uint64) (*types.INode, error)
	WriteINode(i uint64, inode *types.INode) error
	DeleteINode(i uint64) error
}
