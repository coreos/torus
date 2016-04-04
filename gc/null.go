package gc

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

type NullGC struct{}

func (n *NullGC) PrepVolume(_ *models.Volume) error { return nil }
func (n *NullGC) IsDead(ref agro.BlockRef) bool     { return false }
func (n *NullGC) Clear()                            {}
