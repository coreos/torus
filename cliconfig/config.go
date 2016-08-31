package cliconfig

type TorusConfig struct {
	EtcdConfig map[string]etcd_config `json:"etcd_config"`
}

type etcd_config struct {
	Etcd         string `json:"etcd,omitempty"`
	EtcdCaFile   string `json:"etcd-ca-file,omitempty"`
	EtcdCertFile string `json:"etcd-cert-file,omitempty"`
	EtcdKeyFile  string `json:"etcd-key-file,omitempty"`
}
