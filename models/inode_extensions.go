package models

func NewEmptyINode() *INode {
	return &INode{
		Attrs: make(map[string]string),
	}
}
