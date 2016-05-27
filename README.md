# torus

A Go Distributed Storage Engine

See the [wiki](https://github.com/coreos/torus/wiki) for more details

## Overview

Torus is a distributed block storage engine that provides a resource pool and basic file primitives from daemons running atop a cluster. These primitives are made consistent by being append-only and coordinated by [etcd](https://github.com/coreos/etcd). From these primitives, an torus server can support multiple types of volumes, the semantics of which can be broken into subprojects. It ships with a simple block-device volume plugin.

The goal from the start is simplicity; running torus should take at most 5 minutes for a developer to set up and understand, while being as robust as possible. 

Sharding is done via a consistent hash function, controlled in the simple case by a hash ring algorithm, but fully extensible to arbitrary maps, rack-awareness, and other nice features.

## Getting Started

### 0) Build torus

```
go get github.com/coreos/torus
go get -d github.com/coreos/torus
```

Then one of:

```
go install -v github.com/coreos/torus/cmd/torus
go install -v github.com/coreos/torus/cmd/torusctl
go install -v github.com/coreos/torus/cmd/torusblock
```

or 

```
cd $GOPATH/src/github.com/coreos/torus
make
```

Either way you'll find the binaries `torus`, `torusctl` and `torusblock`.

### 1) Get etcd
You need a *v3.0* [etcd](https://github.com/coreos/etcd), as torus uses the v3 API natively and depends on some fixes therein. 
[etcd v3.0.0-beta.0](https://github.com/coreos/etcd/releases/tag/v3.0.0-beta.0) or above is required. 

3.0 natively understands the v3 API. To run a single node cluster locally, etcd is ready to work:

```
etcd --data-dir /tmp/etcd
```

[Clustering etcd v3.0 for high availability is documented by the etcd project](https://github.com/coreos/etcd/blob/master/Documentation/op-guide/clustering.md) but it's a pretty common thing to do if you're running on CoreOS.

### 2) init

We need to initialize the storage keys in etcd. This sets the fixed, global settings for the storage cluster, much like formatting a block device. Fortunately, the default settings should suffice for most cases.

```
torusctl init
```

And you're ready!

If `torusctl` can't connect to etcd, it takes the `-C` flag, just like `etcdctl`

```
torusctl -C $ETCD_IP:2379 init
```

(This remains true for all uses of torus binaries)

If you're curious about the other settings, 
```
torusctl init --help
```
will tell you more.

### 3) Run some storage nodes
#### Running manually
```
./torus --etcd 127.0.0.1:2379 --peer-address http://$MY_IP:40000 --data-dir /path/to/data --size 20GiB
```
This runs a storage node without HTTP. Add `--host` and `--port` to open the HTTP endpoint

(TODO: When gRPC on the same port is stable, default to peer-address for HTTP as well)

Multiple instances can be run, so long as the ports don't conflict and you keep separate data dirs.

#### Running with Docker
##### With Host Networking
```
docker run \
--net=host \
-v /path/to/data1:/data \
-e STORAGE_SIZE=20GiB \
-e LISTEN_HOST=$MY_PUBLIC_IP \
-e LISTEN_HTTP_PORT=4321 \
-e LISTEN_PEER_PORT=40000 \
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
-e ETCD_HOST=127.0.0.1 \
quay.io/coreos/torus
```

#### Running on Kubernetes

In the folder you'll find `torus-daemon-set.yaml`. This example daemonset is almost all you need. 


### 4) Check that everything is reporting in
```
torusctl list-peers
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
torusctl peer add --all-peers
```

You'll notice if you run `torusctl peer list` again, the `MEMBER` column will have changed from `Avail` to `OK`. These nodes are now storing data.

Will immediately impress the peers shown in `list-peers` into service, storing data. Peers can be added one (or a couple) at a time via:

```
torusctl peer add http://$PEER_IP:$PEER_PORT [$PEER_UUID...]
```

To see which peers are in service (and other sharding details):

```
torusctl ring get
```

To remove a node from service:
```
torusctl peer remove http://$PEER_IP:$PEER_PORT
```

Draining of peers will happen automatically. If this is a hard removal (ie, the node is gone forever) just remove it, and data will rereplicate automatically. Doing multiple hard removals above the replication threshold may result in data loss. However, this is common practice to anyone that's ever worked with the fault tolerance in [RAID levels.](https://en.wikipedia.org/wiki/Standard_RAID_levels#Comparison).

Even better fault tolerance with erasure codes and parity is an advanced topic TBD.

### 6) Create a volume

```
torusblock volume create myVolume 10GiB
```

This creates a 10GiB virtual blockfile for use. It will be safely replicated and CRC checked, by default. 

### 7) Mount that volume via NBD

```
sudo modprobe nbd
sudo torusblock --etcd 127.0.0.1:2379 nbd myVolume /dev/nbd0
```

Specifying `/dev/nbd0` is optional -- it will pick the first available.

The mount process is similar to FUSE for a block device; it will disconnect when killed, so make sure it's synced and unmounted.

At this point, you have a replicated, highly-available block device connected to your machine. You can format it and mount it as you'd expect:

```
sudo mkfs.ext4 /dev/nbd0
sudo mount /dev/nbd0 -o discard,noatime /mnt/torus
```

It supports the TRIM SSD command for garbage collecting; `-o discard` enables this.

It is recommended (though not required) to use a log-structured filesystem on these devices, to minimize the chance of corruption. [F2FS](https://en.wikipedia.org/wiki/F2FS) is a good choice, and included in the kernel.
