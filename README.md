# Torus
[![Build Status](https://travis-ci.org/coreos/torus.svg?branch=master)](https://travis-ci.org/coreos/torus)
[![Go Report Card](https://goreportcard.com/badge/github.com/coreos/torus)](https://goreportcard.com/report/github.com/coreos/torus)
[![GoDoc](https://godoc.org/github.com/coreos/torus?status.svg)](https://godoc.org/github.com/coreos/torus)

## Torus Overview

Torus is an open source project for distributed storage coordinated through [etcd](https://github.com/coreos/etcd).

Torus provides a resource pool and basic file primitives from a set of daemons running atop multiple nodes. These primitives are made consistent by being append-only and coordinated by [etcd](https://github.com/coreos/etcd). From these primitives, a Torus server can support multiple types of volumes, the semantics of which can be broken into subprojects. It ships with a simple block-device volume plugin, but is extensible to more.

![Quick-glance overview](Documentation/torus-overview.png)

Sharding is done via a consistent hash function, controlled in the simple case by a hash ring algorithm, but fully extensible to arbitrary maps, rack-awareness, and other nice features. The project name comes from this: a hash 'ring' plus a 'volume' is a torus. 

## Project Status

Development on Torus at CoreOS stopped as of Feb 2017. We started [Torus as a prototype](https://coreos.com/blog/torus-distributed-storage-by-coreos.html) in June 2016 to build a storage system that could be easily operated on top of Kubernetes. We have proven out that model with this project. But, we didn't achieve the development velocity over the 8 months that we had hoped for when we started out, and as such we didn't achieve the depth of community engagement we had hoped for either.

If you have immediate storage needs Kubernetes can plugin to [dozens of other storage options](https://kubernetes.io/docs/user-guide/volumes/) including AWS/Azure/Google/OpenStack/etc block storage, Ceph, Gluster, NFS, etc that are external to Kubernetes.

We are also seeing the emergence of projects, like [rook](https://github.com/rook/rook/tree/master/demo/kubernetes), which creates a storage system that is ran on top of Kubernetes, as an [Operator](https://coreos.com/blog/introducing-operators.html). We expect to see more systems like this in the future, because Kubernetes is a perfect platform for running distributed storage systems.

If you are interested in continuing the project feel free to fork and continue; we can update this README if a particular fork gets solid traction.

For further questions email brandon.philips@coreos.com.

## Trying out Torus

To get started quicky using Torus for the first time, start with the guide to [running your first Torus cluster](Documentation/getting-started.md), learn more about setting up Torus on Kubernetes using FlexVolumes [in contrib](contrib/kubernetes), or create a Torus cluster on [bare metal](https://github.com/coreos/coreos-baremetal/blob/master/Documentation/torus.md).

## Contributing to Torus

Torus is an open source project and contributors are welcome!
Join us on IRC at [#coreos on freenode.net](http://webchat.freenode.net/?channels=%23coreos&uio=d4), [file an issue](https://github.com/coreos/torus/issues) here on Github, check out bigger plans on the [kind/design](https://github.com/coreos/torus/labels/kind%2Fdesign) tag, contribute on bugs that are [low hanging fruit](https://github.com/coreos/torus/labels/low%20hanging%20fruit) for issue ideas and check the [project layout](Documentation/project-layout.md) for a guide to the sections that might interest you.

## Licensing

Unless otherwise noted, all code in the Torus repository is licensed under the [Apache 2.0 license](LICENSE). Some portions of the codebase are derived from other projects under different licenses; the appropriate information can be found in the header of those source files, as applicable.
