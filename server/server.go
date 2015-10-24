package server

import "github.com/barakmich/agro"

type Server struct {
	cold     *agro.MFile
	metadata agro.Metadata
}
