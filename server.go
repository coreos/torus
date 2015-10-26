package agro

import "github.com/barakmich/agro/types"

type Server interface {
	Create(Path, types.Metadata) (File, error)
}
