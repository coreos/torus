// flagconfig is a generic set of flags dedicated to configuring a Torus client.
package flagconfig

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/user"
	"strings"

	"github.com/coreos/torus"
	cli "github.com/coreos/torus/cliconfig"
	"github.com/dustin/go-humanize"
	flag "github.com/spf13/pflag"
)

const (
	defaultTorusConfig = "/.torus/config"
	defaultEtcdAddress = "127.0.0.1:2379"
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
	config            string
	profile           string
)

func AddConfigFlags(set *flag.FlagSet) {
	set.StringVarP(&localBlockSizeStr, "write-cache-size", "", "128MiB", "Maximum amount of memory to use for the local write cache")
	set.StringVarP(&readCacheSizeStr, "read-cache-size", "", "50MiB", "Amount of memory to use for read cache")
	set.StringVarP(&readLevel, "read-level", "", "block", "Read replication level")
	set.StringVarP(&writeLevel, "write-level", "", "all", "Write replication level")
	set.StringVarP(&etcdAddress, "etcd", "C", "", "Address for talking to etcd (default \"127.0.0.1:2379\")")
	set.StringVarP(&etcdCertFile, "etcd-cert-file", "", "", "Certificate to use to authenticate against etcd")
	set.StringVarP(&etcdKeyFile, "etcd-key-file", "", "", "Key for Certificate")
	set.StringVarP(&etcdCAFile, "etcd-ca-file", "", "", "CA to authenticate etcd against")
	set.StringVarP(&config, "config", "", "", "path to cli config file")
	set.StringVarP(&profile, "profile", "", "default", "profile to use in cli config file")

}

func defaultConfig() string {
	usr, err := user.Current()
	if err == nil {
		_, err = os.Stat(usr.HomeDir + defaultTorusConfig)
		if err == nil {
			return usr.HomeDir + defaultTorusConfig
		}
	}
	return ""
}

func loadCliConfig(config string) (*cli.TorusConfig, error) {
	rdata, err := ioutil.ReadFile(config)
	if err != nil {
		return nil, fmt.Errorf("error reading %s: %s\n", config, err)
	}
	var conf *cli.TorusConfig
	err = json.Unmarshal(rdata, &conf)
	if err != nil {
		return nil, fmt.Errorf("error failed to unmarshal %s: %s\n", config, err)
	}
	return conf, nil

}

func BuildConfigFromFlags() torus.Config {
	var err error
	if config == "" {
		config = defaultConfig()
	}
	if config != "" {
		conf, err := loadCliConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			os.Exit(1)
		}
		if etcdAddress == "" {
			etcdAddress = conf.EtcdConfig[profile].Etcd
		}
		if etcdCertFile == "" {
			etcdCertFile = conf.EtcdConfig[profile].EtcdCertFile
		}
		if etcdKeyFile == "" {
			etcdKeyFile = conf.EtcdConfig[profile].EtcdKeyFile
		}
		if etcdCAFile == "" {
			etcdCAFile = conf.EtcdConfig[profile].EtcdCaFile
		}
	}

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

	etcdAddress = defaultEtcdAddress

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
