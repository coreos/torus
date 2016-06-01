# Torus

Torus is an open source project for distributed storage coordinated through [etcd](https://github.com/coreos/etcd).

Torus provides a resource pool and basic file primitives from a set of daemons running atop multiple nodes. These primitives are made consistent by being append-only and coordinated by [etcd](https://github.com/coreos/etcd). From these primitives, a Torus server can support multiple types of volumes, the semantics of which can be broken into subprojects. It ships with a simple block-device volume plugin, but is extensible to more.

![Quick-glance overview](Documentation/torus-overview.png)

Sharding is done via a consistent hash function, controlled in the simple case by a hash ring algorithm, but fully extensible to arbitrary maps, rack-awareness, and other nice features. The project name comes from this: a hash 'ring' plus a 'volume' is a torus. 

## Project Status

Torus is at an early stage and under active development. We do not recommend its use in production, but we encourage you to try out Torus and provide feedback via issues and pull requests. 

## Trying out Torus

To get started quicky using Torus for the first time, start with the guide to [running your first Torus cluster](Documentation/getting-started.md), or learn more about setting up Torus on Kubernetes using FlexVolumes [in contrib](contrib/kubernetes).

## Contributing to Torus

Torus is an open source project and contributors are welcome!
Join us on IRC at [#coreos on freenode.net](http://webchat.freenode.net/?channels=%23coreos&uio=d4), [file an issue](https://github.com/coreos/torus/issues) here on Github, check out bigger plans on the [kind/design](https://github.com/coreos/torus/labels/kind%2Fdesign) tag, contribute on bugs that are [low hanging fruit](https://github.com/coreos/etcd/labels/low%20hanging%20fruit) for issue ideas and check the [project layout](Documentation/project-layout.md) for a guide to the sections that might interest you.

## Licensing

Unless otherwise noted, all code in the Torus repository is licensed under the [Apache 2.0 license](LICENSE). Some portions of the codebase are derived from other projects under different licenses; the appropriate information can be found in the header of those source files, as applicable.
