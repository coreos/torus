package types

func NewEmptyInode() *INode {
	return &INode{
		Attrs: make(map[string]string),
	}
}
