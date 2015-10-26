package agro

import "github.com/barakmich/agro/types"

type Server interface {
	// Standard file path calls.
	Create(Path, types.Metadata) (File, error)

	// Some server metacalls.
	CreateVolume(string) error
	GetVolumes() ([]string, error)
}
