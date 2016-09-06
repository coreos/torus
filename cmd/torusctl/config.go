package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"

	cli "github.com/coreos/torus/cliconfig"
	"github.com/coreos/torus/internal/flagconfig"
	"github.com/spf13/cobra"

	_ "github.com/coreos/torus/metadata/etcd"
)

const (
	defaultTorusConfigDir  = "/.torus"
	defaultTorusConfigFile = "/config.json"
)

var (
	etcdAddress  string
	etcdCertFile string
	etcdKeyFile  string
	etcdCAFile   string
	config       string
	profile      string
	view         bool
)

var configCommand = &cobra.Command{
	Use:   "config",
	Short: "Write config file for torus commands",
	Run:   configAction,
}

func init() {
	configCommand.Flags().StringVarP(&etcdAddress, "etcd", "C", "127.0.0.1:2379", "Address for talking to etcd")
	configCommand.Flags().StringVarP(&etcdCertFile, "etcd-cert-file", "", "", "Certificate to use to authenticate against etcd")
	configCommand.Flags().StringVarP(&etcdKeyFile, "etcd-key-file", "", "", "Key for Certificate")
	configCommand.Flags().StringVarP(&etcdCAFile, "etcd-ca-file", "", "", "CA to authenticate etcd against")
	configCommand.Flags().StringVarP(&config, "config", "", "", "path to torus config file")
	configCommand.Flags().StringVarP(&profile, "profile", "", "default", "profile to use in cli config file")
	configCommand.Flags().BoolVar(&view, "view", false, "view torus configuration and exit")

}

func configAction(cmd *cobra.Command, args []string) {
	var err error
	etcdCfg := cli.Etcd_config{
		Etcd:         etcdAddress,
		EtcdCertFile: etcdCertFile,
		EtcdKeyFile:  etcdKeyFile,
		EtcdCAFile:   etcdCAFile,
	}

	if config == "" {
		usr, err := user.Current()
		if err != nil {
			die("error getting user info: %v", err)
		}
		_ = os.Mkdir(usr.HomeDir+defaultTorusConfigDir, 0700)
		config = filepath.Join(usr.HomeDir + defaultTorusConfigDir + defaultTorusConfigFile)
	}

	if view {
		bdata, err := ioutil.ReadFile(config)
		if err != nil {
			die("error reading config %s: %v", config, err)
		}
		fmt.Println(string(bdata))
		os.Exit(0)
	}

	var c *cli.TorusConfig
	if _, err = os.Stat(config); err != nil {
		c = &cli.TorusConfig{EtcdConfig: map[string]cli.Etcd_config{}}
	} else {
		c, err = flagconfig.LoadConfigFile(config)
		if err != nil {
			die("error loading config file: %v", err)
		}
	}

	c.EtcdConfig[profile] = etcdCfg
	prettyJSON, err := json.MarshalIndent(c, "", "\t")
	if err != nil {
		die("error parsing config file: %v", err)
	}

	err = ioutil.WriteFile(config, prettyJSON, 0600)
	if err != nil {
		die("error writing %s: %v", config, err)
	}
}
