package agro

type Blockset interface {
	Length() int
	GetBlock(i int) ([]byte, error)
	PutBlock(inode INodeRef, i int, b []byte) error

	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
	GetSubBlockset() Blockset
}
