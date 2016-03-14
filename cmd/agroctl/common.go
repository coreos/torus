package main

import (
	"fmt"
	"os"

	"github.com/coreos/agro"
)

func die(why string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, why+"\n", args...)
	os.Exit(1)
}

func mustConnectToMDS() agro.MetadataService {
	cfg := agro.Config{
		MetadataAddress: etcdAddress,
	}
	mds, err := agro.CreateMetadataService("etcd", cfg)
	if err != nil {
		die("couldn't connect to etcd: %v", err)
	}
	return mds
}
