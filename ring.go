package agro

type Ring interface {
	GetBlockPeers(key BlockRef, n int)
	GetINodePeers(key INodeRef, n int)
}
