package etcd

import (
	"io"

	etcdv3 "github.com/coreos/etcd/clientv3"

	"github.com/coreos/agro/models"
)

func (c *etcdCtx) DumpMetadata(w io.Writer) error {
	io.WriteString(w, "## Volumes\n")
	resp, err := c.etcd.Client.Get(c.getContext(), MkKey("volumeid"), etcdv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		v := &models.Volume{}
		v.Unmarshal(x.Value)
		io.WriteString(w, v.String())
		io.WriteString(w, "\n")
	}
	io.WriteString(w, "## INodes\n")
	resp, err = c.etcd.Client.Get(c.getContext(), MkKey("volumemeta", "inode"), etcdv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		v := BytesToUint64(x.Value)
		io.WriteString(w, Uint64ToHex(v))
		io.WriteString(w, "\n")
	}
	io.WriteString(w, "## BlockLocks\n")
	resp, err = c.etcd.Client.Get(c.getContext(), MkKey("volumemeta", "blocklock"), etcdv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		io.WriteString(w, string(x.Value))
		io.WriteString(w, "\n")
	}
	return nil
}
