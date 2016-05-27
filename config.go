package torus

type Config struct {
	DataDir         string
	StorageSize     uint64
	MetadataAddress string
	ReadCacheSize   uint64
	ReadLevel       ReadLevel
	WriteLevel      WriteLevel
}
