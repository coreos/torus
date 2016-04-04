package etcd

import (
	"io"

	"github.com/coreos/agro/models"
)

func (c *etcdCtx) DumpMetadata(w io.Writer) error {
	io.WriteString(w, "## Volumes\n")
	resp, err := c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumeid")))
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
	resp, err = c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumemeta", "inode")))
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		v := bytesToUint64(x.Value)
		io.WriteString(w, uint64ToHex(v))
		io.WriteString(w, "\n")
	}
	io.WriteString(w, "## BlockLocks\n")
	resp, err = c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumemeta", "blocklock")))
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		io.WriteString(w, string(x.Value))
		io.WriteString(w, "\n")
	}

	io.WriteString(w, "## Deadmaps\n")
	resp, err = c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumemeta", "deadmap")))
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		bm := bytesToRoaring(x.Value)
		io.WriteString(w, bm.String())
		io.WriteString(w, "\n")
	}
	io.WriteString(w, "## Open\n")
	resp, err = c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumemeta", "open")))
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		bm := bytesToRoaring(x.Value)
		io.WriteString(w, bm.String())
		io.WriteString(w, "\n")
	}
	io.WriteString(w, "## Dirs\n")
	resp, err = c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("dirs")))
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		dir := &models.Directory{}
		dir.Unmarshal(x.Value)
		io.WriteString(w, dir.String())
		io.WriteString(w, "\n")
	}
	io.WriteString(w, "## Chains\n")
	resp, err = c.etcd.kv.Range(c.getContext(), getPrefix(mkKey("volumemeta", "chain")))
	if err != nil {
		return err
	}
	for _, x := range resp.Kvs {
		io.WriteString(w, string(x.Key)+":\n")
		chains := &models.FileChainSet{}
		chains.Unmarshal(x.Value)
		io.WriteString(w, chains.String())
		io.WriteString(w, "\n")
	}
	return nil
}
