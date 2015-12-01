package agro

type RingType int

type Ring interface {
	GetBlockPeers(key BlockRef, n int) ([]string, error)
	GetINodePeers(key INodeRef, n int) ([]string, error)
	Members() []string

	Describe() string
	Type() RingType
	Version() int

	Marshal() ([]byte, error)
}
