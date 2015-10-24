package agro

import "github.com/barakmich/agro/types"

type Storage interface {
	Flush() error
	Close() error
}

type KeyStorage interface {
	Storage
	GetKey(s string) ([]byte, error)
	WriteKey(s string, data []byte) error
	DeleteKey(s string) error
}

type INodeStorage interface {
	Storage
	GetINode(i uint64) (*types.INode, error)
	WriteINode(i uint64, inode *types.INode) error
	DeleteINode(i uint64) error
}
