package cliconfig

type TorusConfig struct {
	EtcdConfig map[string]Etcd_config `json:"etcd_config"`
}

type Etcd_config struct {
	Etcd         string `json:"etcd,omitempty"`
	EtcdCAFile   string `json:"etcd-ca-file,omitempty"`
	EtcdCertFile string `json:"etcd-cert-file,omitempty"`
	EtcdKeyFile  string `json:"etcd-key-file,omitempty"`
}
