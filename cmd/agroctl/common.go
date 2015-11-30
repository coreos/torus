package main

import (
	"fmt"
	"os"

	"github.com/barakmich/agro"
)

func mustConnectToMDS() agro.MetadataService {
	cfg := agro.Config{
		MetadataAddress: etcdAddress,
	}
	mds, err := agro.CreateMetadataService("etcd", cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't connect to etcd: %s\n", err)
		os.Exit(1)
	}
	return mds
}
