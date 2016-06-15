// flagconfig is a generic set of flags dedicated to configuring a Torus client.
package flagconfig

import (
	"fmt"
	"os"

	"github.com/coreos/torus"
	"github.com/dustin/go-humanize"
	flag "github.com/spf13/pflag"
)

var (
	localBlockSizeStr string
	localBlockSize    uint64
	readCacheSizeStr  string
	readCacheSize     uint64
	readLevel         string
	writeLevel        string
)

func AddConfigFlags(set *flag.FlagSet) {
	set.StringVarP(&localBlockSizeStr, "write-cache-size", "", "128MiB", "Maximum amount of memory to use for the local write cache")
	set.StringVarP(&readCacheSizeStr, "read-cache-size", "", "50MiB", "Amount of memory to use for read cache")
	set.StringVarP(&readLevel, "read-level", "", "block", "Read replication level")
	set.StringVarP(&writeLevel, "write-level", "", "all", "Write replication level")
}

func BuildConfigFromFlags() torus.Config {
	var err error
	readCacheSize, err = humanize.ParseBytes(readCacheSizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing read-cache-size: %s\n", err)
		os.Exit(1)
	}
	localBlockSize, err = humanize.ParseBytes(localBlockSizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing write-cache-size: %s\n", err)
		os.Exit(1)
	}

	var rl torus.ReadLevel
	switch readLevel {
	case "spread":
		rl = torus.ReadSpread
	case "seq":
		rl = torus.ReadSequential
	case "block":
		rl = torus.ReadBlock
	default:
		fmt.Fprintf(os.Stderr, "invalid readlevel; use one of 'spread', 'seq', or 'block'")
		os.Exit(1)
	}

	wl, err := torus.ParseWriteLevel(writeLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	return torus.Config{
		StorageSize:   localBlockSize,
		ReadCacheSize: readCacheSize,
		WriteLevel:    wl,
		ReadLevel:     rl,
	}
}
