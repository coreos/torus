package agro

import (
	"github.com/barakmich/agro"
	"github.com/barakmich/agro/types"
)

type Server interface {
	Create(agro.Path, types.Metadata) error
}
