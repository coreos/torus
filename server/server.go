package server

import "github.com/barakmich/agro"

type Server struct {
	cold     agro.KeyStorage
	metadata agro.Metadata
	inodes   agro.INodeStorage
}
