package torus

import "crypto/tls"

type Config struct {
	DataDir         string
	BlockDevice     string
	StorageSize     uint64
	MetadataAddress string
	ReadCacheSize   uint64
	ReadLevel       ReadLevel
	WriteLevel      WriteLevel

	TLS *tls.Config
}
