package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/barakmich/agro"
	"github.com/barakmich/agro/blockset"
	"github.com/codegangsta/cli"

	_ "github.com/barakmich/agro/metadata/etcd"
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
		MetadataAddress: c.GlobalString("etcd"),
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
	bs := c.String("block-spec")
	if !strings.HasSuffix(bs, ",base") {
		bs += ",base"
	}
	out.DefaultBlockSpec, err = blockset.ParseBlockLayerSpec(bs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing block-spec: %s", err)
		os.Exit(1)
	}
	return out
}
