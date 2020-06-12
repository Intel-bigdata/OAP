<<<<<<< HEAD
OAP - Optimized Analytics Packages for Spark is a project to optimize Spark by providing optimized implmentation of packages in various aspects including cache, shuffle, and so on.

Please refer the below documents for more information.

* [Cache](./oap-cache/README.md)
* [Shuffle](./oap-shuffle/README.md)


## Building OAP

Please refer the below documents for more information.

* [Dev](./dev/README.md)
=======
#  Spark-PMoF: RPMem extension for Spark Shuffle
Spark-PMoF (Persistent Memory over Fabric), RPMem extension for Spark Shuffle, is a Spark Shuffle Plugin which enables persistent memory and high performance fabric technology like RDMA for Spark shuffle to improve Spark performance in shuffle intensive scneario. 

## Contents
- [Introduction](#introduction)
- [Installation](#installation)
- [Benchmark](#benchmark)
- [Usage](#usage)
- [Contact](#contact)

## Introduction

## Installation
Make sure you got [HPNL](https://github.com/Intel-bigdata/HPNL) installed.

```shell
git clone https://github.com/Intel-bigdata/Spark-PMoF.git
cd Spark-PMoF; mvn package
```

## Benchmark

## Usage
This plugin current supports Spark 2.3 and works well on various Network fabrics, including Socket, **RDMA** and **Omni-Path**. Before runing Spark workload, add following contents in spark-defaults.conf, then have fun! :-)

```shell
spark.driver.extraClassPath Spark-PMoF-PATH/target/sso-0.1-jar-with-dependencies.jar
spark.executor.extraClassPath Spark-PMoF-PATH/target/sso-0.1-jar-with-dependencies.jar
spark.shuffle.manager org.apache.spark.shuffle.pmof.RdmaShuffleManager
```

## Contact
Chendi Xue, chendi.xue@intel.com 
Jian Zhang, jian.zhang@intel.com 
>>>>>>> 40287d3a9bb770e07f0da1dca74e20eacd329dd1
