package etcd

import (
	etcdv3 "github.com/coreos/etcd/clientv3"
)

// AtomicModifyFunc is a class of commutative functions that, given the current
// state of a key's value `in`, returns the new state of the key `out`, and
// `data` to be returned to the calling function on success, or an `err`.
//
// This function may be run multiple times, if the value has changed in the time
// between getting the data and setting the new value.
type AtomicModifyFunc func(in []byte) (out []byte, data interface{}, err error)

// TODO(barakmich): Perhaps make this an etcd client library function.
func (c *etcdCtx) AtomicModifyKey(k []byte, f AtomicModifyFunc) (interface{}, error) {
	key := string(k)
	resp, err := c.etcd.Client.Get(c.getContext(), key)
	if err != nil {
		return nil, err
	}
	var version int64
	var value []byte
	if len(resp.Kvs) != 1 {
		version = 0
		value = []byte{}
	} else {
		kv := resp.Kvs[0]
		version = kv.Version
		value = kv.Value
	}
	for {
		newBytes, fval, err := f(value)
		if err != nil {
			return nil, err
		}
		txn := c.etcd.Client.Txn(c.getContext()).If(
			etcdv3.Compare(etcdv3.Version(key), "=", version),
		).Then(
			etcdv3.OpPut(key, string(newBytes)),
		).Else(
			etcdv3.OpGet(key),
		)
		resp, err := txn.Commit()
		if err != nil {
			return nil, err
		}
		if resp.Succeeded {
			return fval, nil
		}
		promAtomicRetries.WithLabelValues(string(key)).Inc()
		kv := resp.Responses[0].GetResponseRange().Kvs[0]
		version = kv.Version
		value = kv.Value
	}
}

//to increase one on the given value.
func BytesAddOne(in []byte) ([]byte, interface{}, error) {
	newval := BytesToUint64(in) + 1
	return Uint64ToBytes(newval), newval, nil
}
