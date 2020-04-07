# ONVAKV: the Optimized Non-Volatile Authenticated Key-Value store

OnvaKV is yet another ADS (Authenticated Data Structure) for blockchains. Like Ethereum's MPT and Cosmos-SDK's IAVL, it is designed as a KV database to store all the state information under consensus. Its main advantage over MPT and IAVL is performance.

MPT and IAVL are built upon traditional KV databases, such as LevelDB or RocksDB. For one single read/write operation, they need to access the underlying KV database for several times. And each time a KV database is accessed, it may access the hard disk for serveral times. Thus, they are very slow.

OnvaKV does not rely on LevelDB or RocksDB during normal read/write/deletion operations. It uses carefully designed data structures in DRAM and hard disk such that reading a KV pair only need to access the hard disk only once. At the same time, it does use RocksDB to store meta information and historical information.

The general idea of OnvaKV is [described here](./docs/OnvaKVIdea.md).

