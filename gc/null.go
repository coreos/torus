package gc

import (
	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
)

type NullGC struct{}

func (n *NullGC) PrepVolume(_ *models.Volume) error { return nil }
func (n *NullGC) IsDead(ref torus.BlockRef) bool     { return false }
func (n *NullGC) Clear()                            {}
