### Running processes

* **Metadata Service (MDS)**
  * A consistent store and distributed lockserver. (etcd)
* **Data Nodes**
  * Provide actual storage (torus)                       -
* **Client Nodes**
  * Connect to data nodes and the MDS to provide access to the storage (eg, torusblock) 

### High-Level Data Model

* Storage blocks (of size 512K by default) 
  * Your data, split into chunks.
* BlockRefs 
  * 192bit identities assigned to a block when it is written at an INode Version Index. Consists of:
    * 24 bits Reference Type (Data, INode, ECC, ...)
    * 40 bits Volume ID
    * 64 bits INode Index
    * 64 bits per-index ID
* INode
  * A list of blocks written within one index timeframe. 
  * A sync() closes the list, and makes sure the blocks are written. 
  * A new INode version gets created, and the  
* Blocklayer/Blockset
  * Extra data, per block inside an INode, that appears as a list of blocks to callers, but offer a hook for more features.
  * Eg:
```
                 | Block 1    | Block 2    | Block 3    | Extra Data
    -----------------------------------------------------------
    CRC Checksums| 2340AE529E | 6432CRE32D | 9CF29F9347 | .....
    Reed-Solomon |     A      |     A      |     A      | [A: BlockRef 20]
    Base         | BlockRef 1 | BlockRef 2 | BlockRef 3 | .....
```
* Rings
  * Pure functions that take a BlockRef and return a permutation of the nodes being used for storage 
* Server
  * Connects to the MDS and underlying storage, acts as an abstraction between the moving parts. 
* File
  * Abstraction over INodes, that support ReadAt(), WriteAt() and Sync()

### Rings, hashing, and recovery

Basically, this looks like a pretty standard [DHT](https://en.wikipedia.org/wiki/Distributed_hash_table)
