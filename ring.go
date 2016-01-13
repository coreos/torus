package agro

type RingType int

type Ring interface {
	GetBlockPeers(key BlockRef) ([]string, error)
	GetINodePeers(key INodeRef) ([]string, error)
	Members() []string

	Describe() string
	Type() RingType
	Version() int

	Marshal() ([]byte, error)
}
