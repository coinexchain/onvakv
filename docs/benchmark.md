

Asgard 1TB SSD, AMD A8-5600K CPU, 8GB DRAM

Create N=4096 Blocks

In each block we create 1000 KV pairs （Key Length = 32 Value Length = 32）

|                 | 创建时间 | size of the database (MB) |
| --------------- | -------- | ------------------------- |
| GoLevelDB       | 31       | 148                       |
| ETH-MPT         | 572      | 2762                      |
| Tendermint-iavl | 2980     | 4590                      |



Create N=4096*32=131072 Blocks

Random Read 131072 KV Pairs

|                 | Time used in creation (sec) | size of the database (MB) | Parrallel Read Time (sec) | Serial Read Time (sec) |
| --------------- | --------------------------- | ------------------------- | ------------------------- | ---------------------- |
| GoLevelDB       | 2168                        | 4620                      | 13.5                      | 14.3                   |
| ETH-MPT         | 75683                       | 133748                    | 65                        | 77                     |
| Tendermint-iavl |                             |                           |                           |                        |

