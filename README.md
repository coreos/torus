# agro

A Go Distributed Storage Engine

See the [wiki](https://github.com/coreos/agro/wiki) for more details

## Overview

Agro is a distributed block storage engine that provides a resource pool and basic file primitives from daemons running atop a cluster. These primitives are made consistent by being append-only and coordinated by [etcd](https://github.com/coreos/etcd). From these primitives, an agro server can support multiple types of volumes, the semantics of which can be broken into subprojects. It ships with a simple block-device volume plugin.

The goal from the start is simplicity; running agro should take at most 5 minutes for a developer to set up and understand, while being as robust as possible. 

Sharding is done via a consistent hash function, controlled in the simple case by a hash ring algorithm, but fully extensible to arbitrary maps, rack-awareness, and other nice features.

## Getting Started

### 0) Build agro

```
go get github.com/coreos/agro
go get -d github.com/coreos/agro
```

Then one of:

```
go install -v github.com/coreos/agro/cmd/agro
go install -v github.com/coreos/agro/cmd/agroctl
go install -v github.com/coreos/agro/cmd/agromount
```

or 

```
cd $GOPATH/src/github.com/coreos/agro
make
```

Either way you'll find the binaries `agro`, `agromount` and `agroctl`.

### 1) Get etcd
You need a *recent* [etcd](https://github.com/coreos/etcd), as agro uses the v3 API natively and depends on some fixes therein. 
[etcd v2.3.0-alpha1](https://github.com/coreos/etcd/releases/tag/v2.3.0-alpha.1) or above is required. 

Make sure to run etcd with the v3 API turned on:
```
etcd --experimental-v3demo --experimental-gRPC-addr 127.0.0.1:2378 --data-dir /tmp/etcd
```

[Clustering etcd v2.3 is left as an exercise to the reader](https://github.com/coreos/etcd/blob/master/Documentation/clustering.md) but it's a pretty common thing to do if you're running on CoreOS.

### 2) mkfs

We need to initialize the storage keys in etcd. This sets the fixed, global settings for the storage cluster, much like formatting a block device. Fortunately, the default settings should suffice for most cases.

```
agroctl mkfs
```

And you're ready!

If `agroctl` can't connect to etcd, it takes the `-C` flag, just like `etcdctl`

```
agroctl -C $ETCD_IP:2378 mkfs
```

(This remains true for all uses of agro binaries)

If you're curious about the other settings, 
```
agroctl mkfs --help
```
will tell you more.

### 3) Run some storage nodes
#### Running manually
```
./agro --etcd 127.0.0.1:2378 --peer-address $MY_IP:40000 --data-dir /path/to/data --size 20GiB
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
quay.io/coreos/agro
```
If you want to run more than one storage node on the host, you can do so by offsetting the ports.

##### Non-host networking
You'll need to figure out non-host networking where all storage nodes are on the same subnet. [Flannel](https://github.com/coreos/flannel), et al, are recommended here. But if you're okay with your docker networking...

```
docker run \
-v /path/to/data1:/data \
-e STORAGE_SIZE=20GiB \
-e ETCD_HOST=127.0.0.1 \
quay.io/coreos/agro
```

#### Running on Kubernetes

In the folder you'll find `agro-daemon-set.yaml`. This example daemonset is almost all you need. 


### 4) Check that everything is reporting in
```
agroctl list-peers
```

Should show your data nodes and their reporting status. Eg:
```
+-----------------+--------------------------------------+---------+------+---------------+--------------+
|     ADDRESS     |                 UUID                 |  SIZE   | USED |    UPDATED    | REB/REP DATA |
+-----------------+--------------------------------------+---------+------+---------------+--------------+
| 127.0.0.1:40000 | babecd8e-d4fc-11e5-a91f-5ce0c5527cf4 | 2.0 GiB | 0 B  | 2 seconds ago | 0 B/sec      |
| 127.0.0.1:40001 | babee2dd-d4fc-11e5-b486-5ce0c5527cf4 | 2.0 GiB | 0 B  | 2 seconds ago | 0 B/sec      |
| 127.0.0.1:40002 | babee99a-d4fc-11e5-a3e3-5ce0c5527cf4 | 1.0 GiB | 0 B  | 2 seconds ago | 0 B/sec      |
| 127.0.0.1:40003 | cb6ee7cb-d4fc-11e5-aff4-5ce0c5527cf4 | 1.0 GiB | 0 B  | 4 seconds ago | 0 B/sec      |
+-----------------+--------------------------------------+---------+------+---------------+--------------+
Balanced: true
```
### 5) Activate storage on the peers

```
agroctl peer add --all-peers
```

Will immediately impress the peers shown in `list-peers` into service, storing data. Peers can be added one (or a couple) at a time via:

```
agroctl peer add $PEER_IP:$PEER_PORT [$PEER_UUID...]
```

To see which peers are in service (and other sharding details):

```
agroctl ring get
```

To remove a node from service:
```
agroctl peer remove $PEER_IP:$PEER_PORT
```

Draining of peers will happen automatically. If this is a hard removal (ie, the node is gone forever) just remove it, and data will rereplicate automatically. Doing multiple hard removals above the replication threshold may result in data loss. However, this is common practice to anyone that's ever worked with the fault tolerance in [RAID levels.](https://en.wikipedia.org/wiki/Standard_RAID_levels#Comparison).

Even better fault tolerance with erasure codes and parity is an advanced topic TBD.

### 6) Create a volume

```
agroctl volume create-block myVolume 10GiB
```

This creates a 10GiB virtual blockfile for use. It will be safely replicated and CRC checked, by default. 

### 7) Mount that volume via NBD

#### Local NBD

```
sudo modprobe nbd
sudo agromount --etcd 127.0.0.1:2378 nbd myVolume /dev/nbd0
```

Specifying `/dev/nbd0` is optional -- it will pick the first available.

The mount process is similar to FUSE for a block device; it will disconnect when killed, so make sure it's synced and unmounted.

At this point, you have a replicated, highly-available block device connected to your machine. You can format it and mount it as you'd expect:

```
sudo mkfs.ext4 /dev/nbd0
sudo mount /dev/nbd0 -o discard,noatime /mnt/agro
```

It supports the TRIM SSD command for garbage collecting; `-o discard` enables this.

It is recommended (though not required) to use a log-structured filesystem on these devices, to minimize the chance of corruption. [F2FS](https://en.wikipedia.org/wiki/F2FS) is a good choice, and included in the kernel.

#### NBD Server

To run an NBD server:

```
agromount --etcd 127.0.0.1:2378 nbdserver
```

The server listens on the standard NBD port by default, 10809.

To use the NBD server, you need an NBD client. (NBD userspace tools)[https://github.com/yoe/nbd] and [QEMU](https://github.com/qemu/qemu).

For example, we'll load a CoreOS image into agro, list the volume via NBD userspace tools, and boot it in QEMU.

Put CoreOS in a 10 GiB agro volume:

```
curl -s -o /tmp/coreos_production_image.bin.bz2 http://alpha.release.core-os.net/amd64-usr/current/coreos_production_image.bin.bz2
bunzip2 /tmp/coreos_production_image.bin.bz2
agromount --etcd 127.0.0.1:2378 load /tmp/coreos_production_image.bin.bz2 coreos-1 10GiB
rm /tmp/coreos_production_image.bin
```

List volumes in the server using NBD userspace tools:

```
nbd-client -l 127.0.0.1                                                                                                                                                                                           
```

You can now boot CoreOS from the NBD server in QEMU. To ensure discard commands are passed through, QEMU needs `discard=on` for the block device, and the mount in the VM needs to be mounted with `-o discard`, as above.


```
sudo qemu-system-x86_64 \
	-enable-kvm \
	-machine q35 \
	-m 1024 \
	-device virtio-scsi-pci,id=scsi \
	-device scsi-hd,drive=hd \
	-drive if=none,discard=on,id=hd,format=raw,file=nbd:127.0.0.1:10809:exportname=coreos-1 \
	-display sdl \
	-serial mon:stdio
```

