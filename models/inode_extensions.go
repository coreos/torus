package models

func NewEmptyInode() *INode {
	return &INode{
		Attrs: make(map[string]string),
	}
}
