### Benchmark of OnvaKV and other ADS

#### Basic Read&Write Test

First, we test the basic read&write performance of OnvaKV and two other ADS: MPT from ethereal and IAVL from tendermint. We also add goleveldb into the test, because MPT and IAVL both use goleveldb as the underlying storage engine.

The hardware for testing is a PC, which has: 

- AMD A8-5600K CPU

- 16GB DDR3 1600MHz DRAM

- Intel Optane 900P 280GB SSD

During Test, we simulate 4096 blocks and during each block 10000 pairs of key-value are created and written. Keys and values are both 32 bytes long. Then we read 327680 KV pairs randomly, in a serial way (one thread) and a parallel way (64 threads).

The test results are shown below:

|                 | Time used in creation (sec) | size of the database (MB) | Parrallel Read Time (sec) | Serial Read Time (sec) |
| --------------- | --------------------------: | ------------------------: | ------------------------: | ---------------------: |
| GoLevelDB       |                         531 |                      2886 |                     10.47 |                  31.72 |
| OnvaKV          |                        1671 |                     18442 |                      1.17 |                   2.60 |
| ETH-MPT         |                       17536 |                     55060 |                     114.5 |                  149.3 |
| Tendermint-iavl |                      222806 |                     92025 |                     453.5 |                  355.8 |

GoLevelDB and OnvaKV are roughly 1\~2 orders faster than MPT, and 2\~3 orders faster than IAVL. They are faster in reading partly because their databases are smaller, and the OS can use DRAM to cache most (if not all) of the database. Even though OnvaKV's database is larger than GoLevelDB, its read performance is far better than GoLevelDB. 

The code for testing can be found at https://github.com/coinexchain/ADS-benchmark and https://github.com/coinexchain/onvakv/tree/master/store/rabbit/rwbench.



#### Transaction Test (Sending Tokens)

Next, we further test OnvaKV's performance by simulating real transactions: sending tokens.

We simulate such a system: there are one native token and 100 fungible tokens, and $n$ accounts, each of which has 1~20 types of tokens. The tokens' amounts are initialized with random numbers. In each transaction, one account sends some amount of a fungible token to another account. The amounts are chosen randomly but with constraints, such that the amounts do not overflow or underflow after sending. 10 native tokens are deducted from the sender's account as gas fee. And the sender's sequence number is increased by one. We keep all the necessary checks in real transactions to keep as close to real scenarios as possible, but these checks are impossible to find errors in the benchmarks.

The first task **Genacc** is generating $n$ random accounts into the system. The generating process will continue for $n/20000$ blocks and during each block 20000 random accounts are generated.

The second task **Checkacc** is reading the $n$ generated accounts out, in a randomized order.

The third task **Runtx** is running $m$ blocks, and each block has 32768 random transactions.

Our first experiments are running a MacBook (15 inch, 2018), which has:

- 2.6GHz six-core (12 threads) Intel Core i7
- 512GB SSD
- 16GB 2400MHz DDR4 DRAM

Because the DRAM is limited, we choose $n=1e8$ and $m=500$. We tested two kinds of binaries: one is built with Golang B-tree and the other is built with C++ B-tree. The time they used to compute the tasks are shown as below (in seconds):

|          | MacBook golang btree | MacBook cppbtree |
| -------- | -------------------- | ---------------- |
| Genacc   | 5402                 | 3116             |
| Checkacc | 2477.5               | 708.8            |
| Runtx    | 1058                 | 472              |

The C++ B-tree is much faster. So in the later tests, we will only use C++ B-tree.

Then we test on AWS's cloud instances with $n=1e8$ and $m=500$. The runtimes are:

|                | i3en.3xlarge (12 vCPUs, 96GB DRAM) | i3en.xlarge (4 vCPUs, 32GB DRAM) |
| -------------- | ----------------------- | ---------------------- |
| Genacc |  2907             |  4404             |
| Checkacc       | 241             |  1946  |
| Runtx  |  154              |  773             |

In Runtx, i3en.3xlarge can reach 500\*32\*1024/154=106387 TPS, and i3en.3xlarge, 21195TPS.

The 3xlarge instance is much faster than large instance, because it's DRAM is so big that almost all the database can be cached.

The DRAM usages are:

|                | i3en.3xlarge (12 vCPUs) | i3en.xlarge (4 vCPUs)  |
| -------------- | ----------------------- | ---------------------- |
| Genacc | 2.6GB             | 2.7GB             |
| Checkacc       | 3.05GB            | 3.10GB |
| Runtx  | 2.71GB              | 2.69GB              |


Finally, we enlarge the account number and transaction number by ten (now  $n=1e9$ and $m=5000$), and run the tests on AWS again.

|                | i3en.3xlarge | i3en.xlarge |
| -------------- | ------------ | --------------------- |
| Genacc | 36901 |                       |
| Checkacc       | 8388 |                       |
| Runtx | 3933 |                       |

In Runtx, i3en.3xlarge can reach 5000\*32\*1024/3933=41657TPS, and i3en.3xlarge, 

The DRAM usages are:

|                | i3en.3xlarge | i3en.xlarge |
| -------------- | ------------ | --------------------- |
| Genacc | 23.9GB |                       |
| Checkacc       |              |                       |
| Runtx | 23.9GB |                       |


When $n=1e8$, the database of OnvaKV takes 127,223 MB, and $n=1e9$, 1,272,671 MB.


