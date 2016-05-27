# A quick overview of the project layout

```
├── block
│   ├── aoe
```

The package for using torus as a block device. A reference example of block device volumes.
`aoe` contains an implementation of an ATA-over-Ethernet server based on a block volume

```
├── blockset
```
Implementations of the Blockset interface.


```
├── cmd
│   ├── torusd
│   ├── torusblk
│   ├── torusctl
│   └── ringtool
```

The `main` functions that each produce a binary. `torusd` is the main server, `torusctl` manipulates and queries multiple servers through etcd, `torusblk` creates, attaches and mounts block devices, and `ringtool` is an experiment for measuring the rebalance properties of multiple rings.

```
├── contrib
│   └── kubernetes
```

Contributions, currently containing a guide for setting up torus on kubernetes

```
├── distributor
│   ├── protocols
│   │   ├── adp
│   │   ├── grpc
│   ├── rebalance
```

Distributor is the package that implements the storage interface, but takes care of all the network requests and distribution. Therefore, it understands various peer-to-peer protocols, and how to rebalance data between other peers. 

```
├── docs
```

You are here!

```
├── gc
```
A separate, small package that can be used in other goroutines to track the liveness of 

```
├── integration
```
Long-running integration tests live here. They spin up a number of virtual nodes, interact with them, and then shut them down.

```
├── internal
│   ├── http
│   └── nbd
```

Packages that are specific to torus. and shouldn't be imported from the outside. `http` defines HTTP routes for torus servers/clients to host, and `nbd` is a hard fork of an NBD library (greatly cleaned up) that may, in the future, be worth splitting into a proper repository.

```
├── metadata
│   ├── etcd
│   └── temp
```

`metadata` holds the implementations of the MDS interface. Currently there's an ephermeral, in-memory temp store (useful for tests) and etcd.

```
├── models
```

Protobufs for serialization and deserialization.

```
├── ring
```

Implementations of the consistent hash ring interface. 

```
├── storage
```
Implementations of underlying storage engines (mmap files, temporary map, potentially bare disks, etc)

