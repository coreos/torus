package agro

// Blockset is the interface representing the standardized methods to interact
// with a set of blocks.
type Blockset interface {
	Length() int
	GetBlock(i int) ([]byte, error)
	PutBlock(inode INodeRef, i int, b []byte) error

	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
	GetSubBlockset() Blockset
}
