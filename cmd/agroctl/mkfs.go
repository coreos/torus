package main

import (
	"fmt"
	"os"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/blockset"
	"github.com/codegangsta/cli"
)

var mkfsCommand = cli.Command{
	Name:   "mkfs",
	Usage:  "prepare a new filesystem by creating the metadata",
	Action: mkfsAction,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "block-size",
			Value: 8196,
			Usage: "size of all data blocks in this filesystem",
		},
		cli.StringFlag{
			Name:  "block-spec",
			Value: "crc",
			Usage: "default replication/error correction applied to blocks in this filesystem",
		},
	},
}

func mkfsAction(c *cli.Context) {
	cfg := agro.Config{
		MetadataAddress: c.String("etcd"),
	}
	md := parseGlobalMetadataFromContext(c)
	err := agro.Mkfs("etcd", cfg, md)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error writing metadata: %s\n", err)
		os.Exit(1)
	}
}

func parseGlobalMetadataFromContext(c *cli.Context) agro.GlobalMetadata {
	out := agro.GlobalMetadata{}
	out.BlockSize = uint64(c.Int("block-size"))
	var err error
	out.DefaultBlockSpec, err = blockset.ParseBlockLayerSpec(c.String("block-spec"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing block-spec: %s", err)
		os.Exit(1)
	}
	fmt.Println(out.DefaultBlockSpec)
	return out
}
