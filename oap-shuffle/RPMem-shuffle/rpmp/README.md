# RPMP(Remote Persistent Memory Pool)
RPMP was designed as a fully disaggregated shuffle solution for distributed compute system, leveraging state-of-art hardware technologies including persist memory and RDMA, but are not necessarily limited to a shuffle solution. It extends local PM(Persistent Memory) to remote PM and targets on a distributed persistent memory pool, providing easy-to-use interfaces, like the malloc and free in standard C library. 

## Contents
- [Installation](#installation)
- [Benchmark](#benchmark)

## Installation
### Build prerequisites
Make sure you got [HPNL](https://github.com/Intel-bigdata/HPNL) installed.

### Build for C/C++
```
git clone https://github.com/Intel-bigdata/Spark-PMoF.git
git submodule update --init --recursive
cd rpmp 
mkdir build
cd build
cmake ..
make && make install
```

## Test
```
cd bin
./unit_tests
```

## Benchmark
### Local 
 - Get local allocate performance
 ```./local_allocate```
### Remote
 - Launch server  
 ```./main -a <server-ip>```
 - Evaluate remote read performance  
 ```./remote_read```
 - Evaluate remote allocate and write performance  
 ```./remote_allocate_write```

