# What to expect under failure conditions

## Single Machine Failures

By default, Torus replicates all data blocks twice.

### Is the replication factor greater than the number of downed nodes?*

#### Yes

*Yes, and these nodes are temporarily down*

No need to panic. At worst, reads and writes will have a somewhat higher latency while the nodes are down. When they come back up, they will catch up and the cluster will proceed as normal.

*Yes, and these nodes are never coming back*

To prevent loss, remove these nodes from the ring:

```
torusctl peer remove UUID-OF-DOWN-NODE
```

You can retrieve that UUID with `torusctl peer list`

Once removed, the cluster will automatically rereplicate and rebalance the data on the live nodes. There will be a slight latency penalty while this process takes place.

#### No

Reads may begin to fail on connected clients, and you may see I/O errors. Writes will be sent to the remaining nodes. If there are no other failures, if the nodes come back, they will catch up and the data will be secure. If they are lost forever, you may experience data loss. 

A future extension may allow peers to optimistically rebalance data when the first nodes stop responding. At the cost of extra bandwidth usage, it can prevent outages.

## Network partition between peers

If sufficient nodes are on the wrong side of the partition, reads may begin to fail, and in-flight writes will sync, but will stop being accepted.

## Network partition between client and etcd

The client will fail to sync and begin reporting I/O errors; this is non-fatal, as the previous sync and related data will remain intact. When the partition is repaired, clients can restart from the checkpoint before the partition and continue; only data written during this timeframe will be lost. In the future, this need not be the case; a client could continue to work until the repair happens, and a sanity check could detect this scenario, saving even the data that was written during the partition.
