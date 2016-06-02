# Running your first Torus cluster

### 0) Get Torus

#### Download a release

Releases are available at http://github.com/coreos/torus/releases

#### Build from master

For builds, Torus assumes a Go installation and a correctly configured [GOPATH](https://golang.org/doc/code.html#Organization). Simply checkout the repo and use the Makefile to build.

```
git clone git@github.com/coreos/torus $GOPATH/src/github.com/coreos/torus
cd $GOPATH/src/github.com/coreos/torus
glide install
make
```

This will create the binaries `torusd`, `torusctl` and `torusblk` in the "bin" directory.

On first build Torus will use [glide](https://github.com/Masterminds/glide) to download its dependenices.

### 1) Get etcd
You need a *v3.0* or higher [etcd](https://github.com/coreos/etcd) instance, as torus uses the v3 API natively and uses the latest client. You might try [etcd v3.0.0-beta.0](https://github.com/coreos/etcd/releases/tag/v3.0.0-beta.0). 

To run a single node cluster locally:

```
etcd --data-dir /tmp/etcd
```

[Clustering etcd for high availability](https://github.com/coreos/etcd/blob/master/Documentation/op-guide/clustering.md) and setting up a production etcd are covered by the etcd team.

### 2) init

We need to initialize Torus in etcd. This sets the global settings for the storage cluster, analogous to "formatting the cluster". The default settings are useful for most deployments.

```
./bin/torusctl init
```

And you're ready!

If `torusctl` can't connect to etcd, it takes the `-C` flag, just like `etcdctl`

```
./bin/torusctl -C $ETCD_IP:2379 init
```

(This remains true for all uses of torus binaries)

If you're curious about the other settings, 
```
./bin/torusctl init --help
```
will tell you more, check the docs, or feel free to ask in IRC.

### 3) Run some storage nodes
#### Running manually
```
./bin/torusd --etcd 127.0.0.1:2379 --peer-address http://127.0.0.1:40000 --data-dir /tmp/torus1 --size 20GiB
./bin/torusd --etcd 127.0.0.1:2379 --peer-address http://127.0.0.1:40001 --data-dir /tmp/torus2 --size 20GiB
```
This runs a storage node without HTTP. Add `--host` and `--port` to open the HTTP endpoint for [monitoring](monitoring.md).

Multiple instances can be run, so long as the ports don't conflict and you keep separate data dirs.

#### Running with Docker
##### With Host Networking
```
docker run \
--net=host \
-v /path/to/data1:/data \
-e STORAGE_SIZE=20GiB \
-e LISTEN_HTTP_PORT=4321 \
-e PEER_ADDRESS=http://127.0.0.1:40000 \
-e ETCD_HOST=127.0.0.1 \
quay.io/coreos/torus
```
If you want to run more than one storage node on the host, you can do so by offsetting the ports.

##### Non-host networking
You'll need to figure out non-host networking where all storage nodes are on the same subnet. [Flannel](https://github.com/coreos/flannel), et al, are recommended here. But if you're okay with your docker networking...

```
docker run \
-v /path/to/data1:/data \
-e STORAGE_SIZE=20GiB \
-e PEER_ADDRESS=http://$NODE_IP:40000 \
-e ETCD_HOST=127.0.0.1 \
quay.io/coreos/torus
```

### 4) Check that everything is reporting in
```
./bin/torusctl list-peers
```

Should show your data nodes and their reporting status. Eg:
```
+------------------------+--------------------------------------+---------+------+--------+---------------+--------------+
|        ADDRESS         |                 UUID                 |  SIZE   | USED | MEMBER |    UPDATED    | REB/REP DATA |
+------------------------+--------------------------------------+---------+------+--------+---------------+--------------+
| http://127.0.0.1:40002 | b529f87e-2370-11e6-97ca-5ce0c5527cf4 | 5.0 GiB | 0 B  | Avail  | 2 seconds ago | 0 B/sec      |
| http://127.0.0.1:40001 | b52a8e4c-2370-11e6-8b0a-5ce0c5527cf4 | 5.0 GiB | 0 B  | Avail  | 2 seconds ago | 0 B/sec      |
| http://127.0.0.1:40000 | b52b8cf6-2370-11e6-8e88-5ce0c5527cf4 | 5.0 GiB | 0 B  | Avail  | 2 seconds ago | 0 B/sec      |
+------------------------+--------------------------------------+---------+------+--------+---------------+--------------+
Balanced: true Usage:  0.00%
```
### 5) Activate storage on the peers

```
./bin/torusctl peer add --all-peers
```

You'll notice if you run `torusctl list-peers` again, the `MEMBER` column will have changed from `Avail` to `OK`. These nodes are now storing data. Peers can be added one (or a couple) at a time via:

```
./bin/torusctl peer add $PEER_ADDRESS [$PEER_UUID...]
```

To see which peers are in service (and other sharding details):

```
./bin/torusctl ring get
```

To remove a node from service:
```
./bin/torusctl peer remove $PEER_ADDRESS
```

Draining of peers will happen automatically. If this is a hard removal (ie, the node is gone forever) just remove it, and data will re-replicate automatically. Doing multiple hard removals above the replication threshold may result in data loss. However, this is common practice if you're familiar with [RAID levels.](https://en.wikipedia.org/wiki/Standard_RAID_levels#Comparison).

Even better fault tolerance with erasure codes and parity is on the roadmap.

### 6) Create a volume

```
./bin/torusblk volume create myVolume 10GiB
```

This creates a 10GiB virtual blockfile for use. It will be safely replicated and CRC checked, by default. 

### 7) Mount that volume via NBD

```
sudo modprobe nbd
sudo ./bin/torusblk --etcd 127.0.0.1:2379 nbd myVolume /dev/nbd0
```

Specifying `/dev/nbd0` is optional -- it will pick the first available device if unspecified.

The mount process is similar to FUSE for a block device; it will disconnect when killed, so make sure it's synced and unmounted.

At this point, you have a replicated, highly-available block device connected to your machine. You can format it and mount it using the standard tools you expect:

```
sudo mkfs.ext4 /dev/nbd0
sudo mount /dev/nbd0 -o discard,noatime /mnt/torus
```

`torusblk nbd` supports the TRIM SSD command to accelerate garbage collecting; the `discard` option enables this.
