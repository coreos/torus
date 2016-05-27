package main

import (
	"fmt"
	"os"

	"github.com/coreos/torus"
)

func die(why string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, why+"\n", args...)
	os.Exit(1)
}

func mustConnectToMDS() torus.MetadataService {
	cfg := torus.Config{
		MetadataAddress: etcdAddress,
	}
	mds, err := torus.CreateMetadataService("etcd", cfg)
	if err != nil {
		die("couldn't connect to etcd: %v", err)
	}
	return mds
}
