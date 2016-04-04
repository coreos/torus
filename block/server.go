package block

import "github.com/coreos/agro"

type BlockServer interface {
	agro.Server
	CreateBlockVolume(name string, size uint64) error
	OpenBlockFile(volume string) (BlockFile, error)
}
