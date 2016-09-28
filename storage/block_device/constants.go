package block_device

var (
	BlockSize    uint64 = 4096 // number of bytes in a device block
	MetadataSize        = BlockSize

	MagicSentence = []byte("I am a torus block device!")
	MagicLen      = len(MagicSentence)
)
