package agro

import "github.com/barakmich/agro/models"

type Server interface {
	// Standard file path calls.
	Create(Path, models.Metadata) (File, error)

	// Some server metacalls.
	CreateVolume(string) error
	GetVolumes() ([]string, error)
}
