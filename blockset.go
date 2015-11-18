package agro

type Blockset interface {
	Length() int
	GetBlock(i int) ([]byte, error)
	PutBlock(i int, b []byte) error

	SetStore(store BlockStore)

	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}
