package gc

import (
	"github.com/coreos/agro"
	"github.com/coreos/agro/models"
)

type nullGC struct{}

func (n *nullGC) PrepVolume(_ *models.Volume) error { return nil }
func (n *nullGC) IsDead(ref agro.BlockRef) bool     { return false }
func (n *nullGC) Clear()                            {}
