# OAP Installation Guide
This document provides information for guiding you how to install OAP on your cluster nodes. Some steps needs to compile and install specicic libraries to your system which requires root access.

## Contents
- [Prerequisites](#Prerequisites)
- [Compiling and Installation](#Compiling and Installation)

## Prerequisites
- [Cmake](https://help.directadmin.com/item.php?id=494)
- [Memkind](https://github.com/Intel-bigdata/memkind)
- [Vmemcache](https://github.com/pmem/vmemcache)
- [HPNL](https://github.com/Intel-bigdata/HPNL)
- [PMDK](https://github.com/pmem/pmdk)  
 To enable Shuffle Remote PMem extension, you must configure and validate RDMA in advance, you can refer to [Shuffle Remote PMem Extension Guide](../oap-shuffle/RPMem-shuffle/README.md) for more details.

## Compiling and Installation
If you have prepared RDMA in accordance with the [Shuffle Remote PMem Extension Guide](../oap-shuffle/RPMem-shuffle/README.md), you can use the following command under the folder dev to automatically install these dependencies.

### Install prerequisites
```shell script
export ENABLE_RDMA=true
source $OAP_HOME/dev/prepare_oap_env.sh
prepare_all
```

If you need't RDMA, just run following command.
```shell script
source $OAP_HOME/dev/prepare_oap_env.sh
prepare_all
```
Some functions to install prerequisties for OAP have been integrated into this prepare_oap_env.sh, you can use command like prepare_cmake to install the specified dependencies after executing the command "source prepare_oap_env.sh". Use the following command to learn more.

```shell script
oap_build_help
```

### Compiling
You can use make-distribution.sh to generate all jars under the dictionary $OAP_HOME/dev/release-package
```shell script
sh $OAP_HOME/dev/make-distribution.sh
``````
If you only want to use specified module of OAP, you can use the command like the following, and then you should find the jar under the dictionary of the module.
```shell script
mvn clean -pl com.intel.oap:oap-cache -am package
```

### Installation
To enable the module which you want to use, please refer to the corresponding documents for the introduction and how to use the feature

* [SQL Index and Data Source Cache](./oap-cache/oap/README.md)
* [RDD Cache PMem Extension](./oap-spark/README.md)
* [Shuffle Remote PMem Extension](./oap-shuffle/RPMem-shuffle/README.md)
* [Remote Shuffle](./oap-shuffle/remote-shuffle/README.md)