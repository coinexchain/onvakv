

Asgard 1TB SSD, AMD A8-5600K CPU, 8GB DRAM

Create N=4096 Blocks

In each block we create 1000 KV pairs （Key Length = 32 Value Length = 32）

|                 | 创建时间 | size of the database (MB) |
| --------------- | -------- | ------------------------- |
| GoLevelDB       | 31       | 148                       |
| OnvaKV          |          |                           |
| ETH-MPT         | 572      | 2762                      |
| Tendermint-iavl | 2980     | 4590                      |







Create N=1024*64=67108 Blocks

Random Read 67108 KV Pairs

|                 | Time used in creation (sec) | size of the database (MB) | Parrallel Read Time (sec) | Serial Read Time (sec) |
| --------------- | --------------------------: | ------------------------: | ------------------------: | ---------------------: |
| GoLevelDB       |                        1159 |                      4607 |                       3.2 |                    7.3 |
| OnvaKV          |                        5428 |                     29541 |                       6.4 |                    7.3 |
| GoLevelDB.new   |                         646 |                      2885 |                      10.5 |                  30.62 |
| OnvaKV.new      |                        1690 |                     18442 |                       1.3 |                    2.5 |
| ETH-MPT         |                       28162 |                     63874 |                        28 |                     37 |
| Tendermint-iavl |                      296342 |                    101750 |                       127 |                    113 |



40960000

real	17m21.924s onvakv

real	12m23.747s goleveldb



218162.4500665779

---



Create N=4096 Blocks

Random Read 327680 KV Pairs



|                 | Time used in creation (sec) | size of the database (MB) | Parrallel Read Time (sec) | Serial Read Time (sec) |
| --------------- | --------------------------: | ------------------------: | ------------------------: | ---------------------: |
| GoLevelDB       |                         531 |                      2886 |                     31.72 |                  10.47 |
| OnvaKV          |                        1671 |                     18442 |                      1.17 |                   2.60 |
| ETH-MPT         |                       17536 |                     55060 |                     114.5 |                  149.3 |
| Tendermint-iavl |                             |                           |                           |                        |







