

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


```


(parted) mklabel gpt                            

(parted) mkpart primary                          

sudo mkfs -t ext4 -q /dev/nvme0n1p1  

**ubuntu@ip-172-31-33-222**:**~**$ sudo mount /dev/nvme0n1p1 /opt/

**ubuntu@ip-172-31-33-222**:**~**$ wget https://golang.org/dl/go1.14.4.linux-amd64.tar.gz

export GOHOME=/home/ubuntu/go

export PATH=$PATH:$GOHOME/bin

export GOPATH=/opt/Go



Sudo apt install g++

```

| **存储优化型 – 最新一代** | EC2               | EMR              |
| ------------------------- | ----------------- | ---------------- |
| i3.xlarge                 | 每小时 0.366 USD  | 每小时 0.078 USD |
| i3.2xlarge                | 每小时 0.732 USD  | 每小时 0.156 USD |
| i3.4xlarge                | 每小时 1.464 USD  | 每小时 0.27 USD  |
| i3.8xlarge                | 每小时 2.928 USD  | 每小时 0.27 USD  |
| i3.16xlarge               | 每小时 5.856 USD  | 每小时 0.27 USD  |
| i3en.xlarge               | 每小时 0.532 USD  | 每小时 0.113 USD |
| i3en.2xlarge              | 每小时 1.064 USD  | 每小时 0.226 USD |
| i3en.3xlarge              | 每小时 1.596 USD  | 每小时 0.27 USD  |
| i3en.6xlarge              | 每小时 3.192 USD  | 每小时 0.27 USD  |
| i3en.12xlarge             | 每小时 6.384 USD  | 每小时 0.27 USD  |
| i3en.24xlarge             | 每小时 12.768 USD | 每小时 0.27 USD  |





| Instance Size    | 100% Random Read IOPS | Write IOPS  |
| :--------------- | :-------------------- | :---------- |
| `i3.large` *     | 100,125               | 35,000      |
| `i3.xlarge` *    | 206,250               | 70,000      |
| `i3.2xlarge`     | 412,500               | 180,000     |
| `i3.4xlarge`     | 825,000               | 360,000     |
| `i3.8xlarge`     | 1.65 million          | 720,000     |
| `i3.16xlarge`    | 3.3 million           | 1.4 million |
| `i3.metal`       | 3.3 million           | 1.4 million |
| `i3en.large` *   | 42,500                | 32,500      |
| `i3en.xlarge` *  | 85,000                | 65,000      |
| `i3en.2xlarge` * | 170,000               | 130,000     |
| `i3en.3xlarge`   | 250,000               | 200,000     |
| `i3en.6xlarge`   | 500,000               | 400,000     |
| `i3en.12xlarge`  | 1 million             | 800,000     |
| `i3en.24xlarge`  | 2 million             | 1.6 million |
| `i3en.metal`     | 2 million             | 1.6 million |

\* For these instances, you can get up to the specified performance.





|          | i3en.3xlarge | i3en.xlarge |
| -------- | ------------ | ----------- |
| Genacc   | 36901        |             |
| Checkacc | 8388         |             |
| Runtx    | 3933         |             |

If we increate worker count from 64 to 128, the time used by Runtx changes from 3933 to 4005, a little slower