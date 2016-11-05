An inode is the unit of the append-only log for the DFS. Inode numbers are ever-increasing. If inode IDs are 64-bit integers, we have enough writes to last us till doomsday (one inode sync() per millisecond == 584.5 million years)

A file is merely the 'head' version of its inode, hence the revision being the atomic unit. An inode for one file may invalidate previous inodes without requiring they be fully deleted immediately. This allows for locks and simpler concurrency (In the case of multiple writers: I may open a file at inode revision 104, it may be replaced by 105, but I can keep serving 104 to my client until it updates). 
In terms of how syncs get resolved, last write wins, as per fairly usual UNIX semantics. (run `echo "foo" > foo` simultaneously and see what happens). More intricate interactions (open a file for append-only, `flock()`, et al) are possible with this model but not on the immediate roadmap.

An inode consists of the following data:

* Replaced inode number
* File Blockset

Further abstractions can be built around this, say a POSIX file:

* INode
* Filenames that refer to it (hard links)
* Metadata
  * Owner, Group
  * Permissions
  * Extended Attrs

Inodes are, themselves, serialized and stored as blocks in the volume. A special bit is set to identify them as inodes, and to separate their ID space from the blocks of data they represent. To wit:

* Type(data) Volume 1, Inode Index 2, Index 1 (first block of data written at index 2)
* Type(inode) Volume 1, Inode Index 2, Index 1 (first block of inode serialization written at index 2)

At this point, the actual data blocks form a traditional sharded KV store. We reconstruct the file by asking for the appropriate keys of the appropriate block ranges (blocks are a fixed size). Replication is handled through successive members in our hash function (which we have opportunity to define).

An inode can be committed on explicit fsync() -- note they have no notion of what number they represent. Therefore, multiple writes can happen to a local version of the inode (a "staging" inode) before committing it to the greater cluster.

The ID list need not be contiguous. Deleted blocks may exist in the logical keyspace, but be unreferred to in the latest version of the inode. The simplest GC looks at the current inode and older (but not newer) and deletes blocks that aren't referenced.
