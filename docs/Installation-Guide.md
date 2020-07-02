# OAP Installation Guide
This document provides information for guiding you how to install OAP on your cluster nodes. Some steps needs to compile and install specicic libraries to your system which requires root access.

## Contents
- [OAP Installation Guide](#oap-installation-guide)
  - [Contents](#contents)
  - [Prerequisites](#prerequisites)
      - [Install prerequisites](#install-prerequisites)
    - [Compiling OAP](#compiling-oap)
    - [Configuration](#configuration)

## Prerequisites 

- **OS** : Fedora 29  
To enable Shuffle Remote PMem extension, you must configure and validate RDMA in advance, you can refer to [Shuffle Remote PMem Extension Guide](../oap-shuffle/RPMem-shuffle/README.md#4-configure-and-validate-rdma) for more details.

####  Install prerequisites 
Some dependencies required by OAP listed below, to enable OAP, you must install all of them on your cluster nodes.
- [Cmake](https://help.directadmin.com/item.php?id=494)
- [Memkind](https://github.com/Intel-bigdata/memkind)
- [Vmemcache](https://github.com/pmem/vmemcache)
- [HPNL](https://github.com/Intel-bigdata/HPNL)
- [PMDK](https://github.com/pmem/pmdk)  
- [GCC > 7](https://gcc.gnu.org/wiki/InstallingGCC)  
You can refer to the corresponding documents to install prerequisites by yourself. If your system is Fedora 29 or Centos 7, we have provided a shell script named prepare_oap_env.sh to install all these dependencies.

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
If you only want to use specified module of OAP, you can refer to the corresponding user guide. Some functions to install prerequisites for OAP have been integrated into this prepare_oap_env.sh, you can use command like prepare_cmake to install the specified dependencies after executing the command "source prepare_oap_env.sh". Use the following command to learn more.  
```shell script
oap_build_help
```
If there are any problems during the automatic installation process, we recommend you to refer to the relevant documentation of the dependency, and then install it by yourself.


### Compiling OAP
If you have installed all prerequisites, you can download our pre-built package [oap-product-0.8.1-bin-spark-2.4.4.tar.gz](https://github.com/Intel-bigdata/OAP/releases/download/v0.8.1-spark-2.4.4/oap-product-0.8.1-bin-spark-2.4.4.tar.gz)  to your working node, unzip it and put the jars to your working directory such as `/home/oap/jars/`, and put the `oap-common-0.8.1-with-spark-2.4.4.jar` to the directory `$SPARK_HOME/jars/`. If you’d like to build from source code,  you can use make-distribution.sh to generate all jars under the dictionary $OAP_HOME/dev/release-package.
```shell script
sh $OAP_HOME/dev/make-distribution.sh
``````
If you only want to build specified module of OAP, you can use the command like the following, and then you should find the jar under the dictionary of the module.
```shell script
# build oap-cache
mvn clean -pl com.intel.oap:oap-cache -am package 
```

###  Configuration
To enable the module which you want to use, please refer to the corresponding documents for the introduction and how to use the feature.

* [Optimized Analytics Package for Spark\* Platform (OAP)](../README.md#user-guide)