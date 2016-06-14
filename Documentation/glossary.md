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
A logical list of blocks. Appears as a linear array of blocks, but may hold other data (or other blocks) as well, to enable certain redundancy, availability, and data protection properties.

For example, the base block layer is just an array of blocks. The CRC block layer has a layer beneath it to represent the array of blocks, and holds a CRC hash to the side for each of the blocks as they are read or written.

### Blockset
A stack of BlockLayers.

