package agro

type RingType int

type Ring interface {
	GetBlockPeers(key BlockRef, n int)
	GetINodePeers(key INodeRef, n int)

	Describe() string
	Type() RingType
	Version() int
}
