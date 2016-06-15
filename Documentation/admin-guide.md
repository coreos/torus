# Torus Admin Guide

## I want to...

### Set up Torus

#### Set up Torus directly

See the root README.md for a pretty good overview.

#### Set up Torus on a new Kubernetes cluster

See contrib/kubernetes/README.md

#### Set up the Torus FlexVolume Plugin on an existing Kubernetes cluster

The default path for installing flexvolume plugins is `/usr/libexec/kubernetes/kubelet-plugins/volume/exec/` -- so on every node running the kubelet, you'll need to create the subfolder:

```
mkdir -p /usr/libexec/kubernetes/kubelet-plugins/volume/exec/coreos.com~torus/
```

The `torusblk` binary itself conforms as to the flexVolume api, so you'll want to copy it, named `torus`, inside that directory (as per the [Kubernetes repo](https://github.com/kubernetes/kubernetes/tree/master/examples/flexvolume)):

```
cp ./torusblk /usr/libexec/kubernetes/kubelet-plugins/volume/exec/coreos.com~torus/torus 
```

And restart the kubelet so that it registers the new plugin, eg (on systemd systems):

```
systemctl restart kubelet
```

### Use Block Volumes

All the following commands take an optional `-C HOST:PORT` for your etcd endpoint, if it's not localhost.

#### List all volumes

```
torusctl volume list
```

#### Provision a new block volume

```
torusctl volume create-block VOLUME_NAME SIZE
```
or equivalently
```
torusctl block create VOLUME_NAME SIZE
```

Where VOLUME_NAME is whatever you prefer, as long as there's not already one named the same. 

SIZE is given in bytes, and supports human-readable suffixes: M,G,T,MiB,GiB,TiB; so for a 1 gibibyte drive, you can use `1GiB`.

#### Delete a block volume

```
torusctl volume delete VOLUME_NAME
```

#### Attach a block volume

``
torusblk nbd VOLUME_NAME [NBD_DEVICE]
``

NBD_DEVICE is optional. Other options for serving or attaching a block device may appear here in the future.

`torusblk nbd` will block until it recieves a signal, which will disconnect the volume from the device. It's recommended to run this under an init process if you wish to detach it from your terminal.

#### Mount/format a block volume

Once attached to a device (which is reported when `torusblk nbd` starts), it works like any block device; so standard tools like `mkfs` and `mount` will work.

### Modify my cluster

Again, all the following commands take an optional `-C HOST:PORT` for your etcd endpoint, if it's not localhost.

#### Add a storage node

*Let the storage node add itself*

The `--auto-join` flag is there for this reason. When we start the node with it, eg:

```
./torusd --etcd 127.0.0.1:2379 --peer-address http://$MY_IP:40000 --data-dir /path/to/data --size 20GiB --auto-join
```

it will join the cluster and data will start rebalancing onto this new node.

*Manually add a storage node*

If there's an available node that is not part of the storage set, it will appear as "Avail" in `torusctl peer list`. It can be added by:

```
torusctl peer add ADDRESS_OF_NODE
```

or

```
torusctl peer add UUID_OF_NODE
```

#### Remove a storage node

Removing is as easy as adding a node:

```
torusctl peer remove ADDRESS_OF_NODE
```

or

```
torusctl peer remove UUID_OF_NODE
```

Data will immediately start migrating off the node, or replicating from other sources if the node is completely lost.

#### Change replication

```
torusctl ring set-replication AMOUNT
```

Where amount is the number of machines expected to hold a copy of any block. `2` is default.

#### Manually edit my hash ring

**ADVANCED**: Do not attempt unless you're sure of what you're doing. If you're doing this often, there's probably some better tooling that needs to be created that's worth filing a bug about.

```
torusctl ring manual-change --help 
```

Will show the options.
* `--type` will change the type of ring
* `--replication` sets the replication factor
* `--uuids` is a comma-separated list of the UUIDs with associated data dirs.

Join us in IRC if you'd like to chat about ring design.

