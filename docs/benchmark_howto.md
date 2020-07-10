### How to run benchmarks about OnvaKV



Before you run any benchmarks, please enlarge the count of opened files:

```bash
ulimit -n 32768 # on ubuntu
```

On MacOS, 32768 is not accepted, you can use 20000 instead.

Step 0: Download a file as random source:

``` bash
cd ~/Downloads
wget https://golang.org/dl/go1.14.4.linux-amd64.tar.gz
```

Step 1: clone the repos:

``` bash
git clone https://github.com/coinexchain/ADS-Benchmark.git
git clone https://github.com/coinexchain/onvakv
```

Step 2: run read&write benchmarks for GoLevelDB:

``` bash
cd ADS-Benchmark/goleveldb/
go build
time ./goleveldb w ~/Downloads/go1.14.4.linux-amd64.tar.gz $((40960*10000)) # write
time ./goleveldb rs sample.txt `wc sample.txt  |gawk '{print $1}'` # serial read
time ./goleveldb rp sample.txt `wc sample.txt  |gawk '{print $1}'` # parrallel read
```

Step 3: run read&write benchmarks for OnvaKV:

``` bash
cd onvakv/store/rabbit/rwbench/
go build -tags cppbtree
export RANDFILE=~/Downloads/go1.14.4.linux-amd64.tar.gz
/usr/bin/time -v ./rwbench w ~/Downloads/go1.14.4.linux-amd64.tar.gz $((4096*10000)) # write
/usr/bin/time -v ./rwbench rs sample.txt `wc sample.txt  |gawk '{print $1}'` # serial read
/usr/bin/time -v ./rwbench rp sample.txt `wc sample.txt  |gawk '{print $1}'` # parrallel read
```

The test uses about 30GB disk.

The `-v` option of /usr/bin/time is only supported in linux, which can show the memory usage of a program. On MacOS, you can use gnu time `gtime` instead. Please use `brew install gnu-time` to install it.

Step 4: run transaction benchmarks for OnvaKV:

``` bash
cd onvakv/store/rabbit/accbench/
go build -tags cppbtree
/usr/bin/time -v ./accbench genacc $((10000*10000))  # create accounts
/usr/bin/time -v ./accbench checkacc $((10000*10000))  # read and check accounts
/usr/bin/time -v ./accbench gentx $((10000*10000)) 500 # generate 500 blocks of transactions
/usr/bin/time -v ./accbench runtx 500 # run 500 blocks of transactions
```

The test uses about 150GB disk.