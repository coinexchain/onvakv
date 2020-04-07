### Introduction to packages of OnvaKV

Here we introduce the packages and the data structres defined in them.

OnvaKV has three building blocks: index-tree, data-tree, and meta-db.

```
             +----------------+
             |                |
             |     OnvaKV     |
             |                |
             +----------------+
              /     /        \
             /     /          \ 
            /     /            \
           /   metadb           \
          /       |            datatree
      indextree   |            /      \
        /  \      |           /        \
       /    \     |   entry-file     twig-merkle-tree-file
      /      \    |           \         /
     /        \   |            \       /
    B-Tree   RocksDB            \     /
   (cpp&go)                      \   /
                           Head-Prunable File
```

The API of OnvaKV is somehow hard to use. It must be wrapped by some "store" data structures to provide an easy-to-used KV-style API. The figure below shows the relationship among these "store" data structures.

```
  PrefixedStore
         |
         |
    MultiStore
      |      \
      |       \
   TrunkStore  \
      |   \     \
      |    \     \
  RootStore \     \
      |     CacheStore
      |
   OnvaKV
```

These data structures will be introduced as follows.

#### B-tree

We include two B-tree implementations. One is a Golang version from modernc.org/b (indextree/b/btree_nocgo.go), and the other is a C++ version from Google (indextree/b/cppbtree/btree.go and indextree/b/btree_cgo.go). Why there are two versions? Because we do not have 100% confidence of either one of them. So we use fuzz test to compare their outputs. If the outputs are the same, then most likely they are both correct, because they are implemented independently and hard to have the same bug.

In production, the C++ version is preferred. Because:

1. The B-Tree will use a lot of memory. The C++ version doesn't use GC, so it will be faster.
2. We can use a trick to save memory when the key's length is 8.

This trick is old one. It utilizes the fact that pointers are aligned, that is, the least significant two bits is always zero. So, when the key's length is 8 and the least significant two bits of its first byte is not 2'b00, we do not need to store a pointer to byte array, instead, we can store the byte array within the 8 bytes occupied by a pointer. (Note that x86 and arm uses little endian, which means the least significant two bits of an int64 locate in the first byte of an array.)

Why we use B-Tree. Because it's much more memory-efficient that Red-Black tree and is cache friendly.

### RocksDB

See indextree/rocks_db.go

RocksDB is a KV database written in C++. It has a Golang binding, which is not so easy to use. Tendermint wraps the binding for easier use. We refined Tendermint's wrapper to support pruning.

RocksDB supports filtering during compaction, which is a unique feature among all the opensource KV databases.  Indextree uses "original key + 8 bytes of expiring height" as keys when writing to RocksDB. So if we no longer need the KV-pairs whose expiring height are old enough, they can be filtered out during compaction: this is how pruning works.

#### indextree

See indextree/indextree.go

Here we implement IndexTree with an in-memory B-Tree and a on-disk RocksDB.  The B-Tree contains only the latest key-position records, while the RocksDB contains several versions of positions for each key. The B-Tree's keys are original keys while the keys in RocksDB have two parts: the original key and 64-bit height. The height means the key-position record expires (get invalid) at this height. When the height is math.MaxUInt64, the key-position record is up-to-date, i.e., not expired.

When we execute the transactions in blocks, only the latest key-position records are used, which can be queried fast from in-memory B-tree. The RocksDB is only queried when we need historical KV pairs. Of cause, as executing the transactions, the RocksDB is written frequently, but its write performance is much higher than random read.

The RocksDB's content is also used to initialize the B-Tree when starting up. When height is math.MaxUInt64, the KV pair is up-to-date and must be inserted to the B-Tree.

#### metadb

See metadb/metadb.go

We need to store a little meta information when a block finishes its execution. Since the data size is not large, we can use any method to store them. The best way to store them is to reuse indextree's RocksDB, such that we can get the per-block atomic behavior between indextree and metadb. (Note currently it is not implemented this way, and we will change)

When OnvaKV is not properly closed, we should use the information in metadb as guide to recover the other parts of OnvaKV.

#### Head-Prunable File

See datatree/hpfile.go

Normal files can not be pruned(truncated) from the beginning to some middle point. HPFile use a sequence of small files to simulate one big file. Thus, pruning from the beginning is to delete the first several small files.

#### Entry File

See datatree/entryfile.go

It uses HPFile to store entries, i.e., the leaves of the data tree.

#### Twig Merkle Tree File

See datatree/twigmtfile.go

It uses HPFile to store small 2048-leave small Merkle tree in a twig.

#### Datatree

See datatree/tree.go

This is most important data structures. Youngest twig, active twigs and pruned twigs are all implemented here. Most of the code are related to incrementally modify the Merkle tree when modifying its leaves, which are performed in a batch way after each block.

#### Top of OnvaKV

See onvakv.go

It integrates the three major parts and implement the basic read/update/insert/delete operations. The most important job of it is to keep these parts in synchronous. It uses a two-phase protocol: during the execution of transactions, it perform parrallel prepareations to load necessary information in DRAM; when a block is committed, it updates these parts in a batch way.

#### Store Data Structures

CacheStore (store/cache.go) It use golang version of B-tree to implement an in-memory cache for overlay. It is better than Cosmos-SDK's implementation of map and on-the-fly sorting.

RootStore (store/root.go) It survives many blocks to provide persistent cache for frequently-reused data.

TrunkStore (store/trunk.go) It is for one block's execution, with the cache overlay.

MultiStore (store/multi.go) It is for one transaction's execution, with the cache overlay.

PrefixedStore (store/prefix.go) It is used by a "keeper", implemented by prefixing a MultiStore.



#### Memory Usage

Supposing all the keys are 8 bytes long, then the B-Tree in indextree use 22bytes per key, averagely.

Active twig needs 2048+1024+512+256+256+256 = 4352bits. And it also needs roughly 256bits in upper level tree. So an entry uses (4352+256)/8/2048 = 0.281bytes. Supposing active bits and inactive bits are 1:1, then an active KV-pair uses 2*0.281 = 0.56bytes.

So totally a KV-pair uses 22.56bytes on average.