// flagconfig is a generic set of flags dedicated to configuring a Torus client.
package flagconfig

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"strings"

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
	etcdAddress       string
	etcdCertFile      string
	etcdKeyFile       string
	etcdCAFile        string
)

func AddConfigFlags(set *flag.FlagSet) {
	set.StringVarP(&localBlockSizeStr, "write-cache-size", "", "128MiB", "Maximum amount of memory to use for the local write cache")
	set.StringVarP(&readCacheSizeStr, "read-cache-size", "", "50MiB", "Amount of memory to use for read cache")
	set.StringVarP(&readLevel, "read-level", "", "block", "Read replication level")
	set.StringVarP(&writeLevel, "write-level", "", "all", "Write replication level")
	set.StringVarP(&etcdAddress, "etcd", "C", "", "Address for talking to etcd")
	set.StringVarP(&etcdCertFile, "etcd-cert-file", "", "", "Certificate to use to authenticate against etcd")
	set.StringVarP(&etcdKeyFile, "etcd-key-file", "", "", "Key for Certificate")
	set.StringVarP(&etcdCAFile, "etcd-ca-file", "", "", "CA to authenticate etcd against")

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

	cfg := torus.Config{
		StorageSize:     localBlockSize,
		ReadCacheSize:   readCacheSize,
		WriteLevel:      wl,
		ReadLevel:       rl,
		MetadataAddress: etcdAddress,
	}
	etcdURL, err := url.Parse(etcdAddress)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid etcd address: %s", err)
		os.Exit(1)
	}

	if etcdCertFile != "" {
		etcdCert, err := tls.LoadX509KeyPair(etcdCertFile, etcdKeyFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "couldn't load cert/key: %s", err)
			os.Exit(1)
		}
		caPem, err := ioutil.ReadFile(etcdCAFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "couldn't load trusted CA cert: %s", err)
			os.Exit(1)
		}
		etcdCertPool := x509.NewCertPool()
		etcdCertPool.AppendCertsFromPEM(caPem)
		cfg.TLS = &tls.Config{
			Certificates: []tls.Certificate{etcdCert},
			RootCAs:      etcdCertPool,
			ServerName:   strings.Split(etcdURL.Host, ":")[0],
		}
	}

	return cfg
}
