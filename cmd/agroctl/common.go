package main

import (
	"fmt"
	"os"

	"github.com/barakmich/agro"
	"github.com/codegangsta/cli"
)

func mustConnectToMDS(c *cli.Context) agro.MetadataService {
	cfg := agro.Config{
		MetadataAddress: c.GlobalString("etcd"),
	}
	mds, err := agro.CreateMetadataService("etcd", cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't connect to etcd: %s\n", err)
		os.Exit(1)
	}
	return mds
}
