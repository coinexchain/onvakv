

Asgard 1TB SSD, AMD A8-5600K CPU, 8GB DRAM

Create N=4096 Blocks

In each block we create 1000 KV pairs （Key Length = 32 Value Length = 32）

|                 | 创建时间 | size of the database (MB) |
| --------------- | -------- | ------------------------- |
| GoLevelDB       | 31       | 148                       |
| ETH-MPT         | 572      | 2762                      |
| Tendermint-iavl | 2980     | 4590                      |







Create N=1024*64=67108 Blocks

Random Read 67108 KV Pairs

|                 | Time used in creation (sec) | size of the database (MB) | Parrallel Read Time (sec) | Serial Read Time (sec) |
| --------------- | --------------------------: | ------------------------: | ------------------------: | ---------------------: |
| GoLevelDB       |                         968 |                      2369 |                       6.2 |                    7.8 |
| ETH-MPT         |                       28162 |                     63874 |                        28 |                     37 |
| Tendermint-iavl |                      296342 |                    101750 |                       127 |                    113 |

