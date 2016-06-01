### Peer
A server implementation that stores data.

### [Direct] Client
A server-like implementation that doesn't store, but talks on the internal port to Peers.

### Proxy Client
A consumer of the HTTP endpoints.

### Ring
A collection of Peers arranged in a ring to facilitate distribution of storage and work.

### MDS
Metadata Service. A provider of globally-consistent metadata for Peers and Clients to rely on.

### INodeStore
Storage for file-system level information.

### BlockStore
A low-level block storage provider.

### BlockLayer
An arrangement of blocks which can enable certain redundancy, availability, and data protection properties.

### Blockset

