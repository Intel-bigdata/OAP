# OAP Installation Guide
This document provides information for guiding you how to compile OAP and its dependencies, and install them on your cluster nodes. Some steps needs to compile and install specicic libraries to your system which requires root access.

## Contents
  - [Prerequisites](#prerequisites)
      - [Install prerequisites](#install-prerequisites)
  - [Compiling OAP](#compiling-oap)
  - [Configuration](#configuration)

## Prerequisites 

- **OS Requirements**  
We have tested OAP on Fedora 29 and CentOS 7.6. We remmend you use Fedora 29 and CentOS 7.6 or above.

- **Requirements for Shuffle Remote PMem Extension**  
If you want to use Shuffle Remote PMem Extension, you need to configure and validate RDMA before this installation steps. You can refer to [Shuffle Remote PMem Extension Guide](../oap-shuffle/RPMem-shuffle/README.md#4-configure-and-validate-rdma) for the details of configuring and validating RDMA.

###  Install prerequisites 
Some dependencies required by OAP listed below, to use OAP, you must compile and install these libraries on every cluster nodes. We provide a script desbired below to help you to compile and install all these libraries. 
- [Cmake](https://help.directadmin.com/item.php?id=494)
- [Memkind](https://github.com/Intel-bigdata/memkind)
- [Vmemcache](https://github.com/pmem/vmemcache)
- [HPNL](https://github.com/Intel-bigdata/HPNL)
- [PMDK](https://github.com/pmem/pmdk)  
- [GCC > 7](https://gcc.gnu.org/wiki/InstallingGCC)  

If you are an expert on compiling and installing c/C++ libraies, you can refer to the corresponding documents of these libraries to install by yourself. If you are not an expert, we provided a shell script named prepare_oap_env.sh to help you to complete compling and installing all these dependencies if your system is Fedora 29 or Centos 7.6 or above.

Clone the OAP to your local directory:

```
git clone -b branch-0.8-spark-2.4.x  https://github.com/Intel-bigdata/OAP.git
cd OAP
```

**Note: The following prepare process needs to run as the root user. And assume you run the following commands under the OAP home directory.**

If you want to use Shuffle Remote PMem Extension feature and have completed the RDMA configuring and validating steps, execute the following commands to run the preparing process:
```shell script
export ENABLE_RDMA=true
source ./dev/prepare_oap_env.sh
prepare_all
```

If you don't want to use Shuffle Remote PMem Extension feature, you can execute the following commands to run the preparing process:
```shell script
export ENABLE_RDMA=false
source ./dev/prepare_oap_env.sh
prepare_all
```
If you only want to use specified module of OAP, you can refer to the compling and installing steps in the feature user guide. Some functions to install prerequisites for OAP have been integrated into this prepare_oap_env.sh, you can use command like prepare_cmake to install the specified dependencies after executing the command "source prepare_oap_env.sh". Use the following command to learn more.  

```shell script
oap_build_help
```
If there are any problems during the above preparing process, we recommend you to refer to the library documentation listed above, and install it by yourself.


## Compiling OAP
If you have installed all prerequisites, you can download our pre-built package [oap-0.8.1-bin-spark-2.4.4.tar.gz](https://github.com/Intel-bigdata/OAP/releases/download/v0.8.1-spark-2.4.4/oap-0.8.1-bin-spark-2.4.4.tar.gz)  to your working node, unzip it and put the jars to your working directory such as `/home/oap/jars/`, and put the `oap-common-0.8.1-with-spark-2.4.4.jar` to the directory `$SPARK_HOME/jars/`. If you’d like to build from source code,  you can use make-distribution.sh to generate all jars under the dictionary ./dev/release-package in OAP home.
```shell script
sh ./dev/make-distribution.sh
``````
If you only want to build specified module of OAP, you can use the command like the following, and then you should find the jar under the dictionary of the module.
```shell script
# build SQL Index & Data Source Cache module
mvn clean -pl com.intel.oap:oap-cache -am package 
```

```shell script
# build Shuffle Remote PMem Extension module
mvn clean -pl com.intel.oap:oap-rpmem-shuffle -am package 
```

```shell script
# build RDD Cache PMem Extension module
mvn clean -pl com.intel.oap:oap-spark -am package 
```

```shell script
# build Remote Shuffle module
mvn clean -pl com.intel.oap:oap-remote-shuffle -am package 
```

##  Configuration
After you have successfully completed the installation of prerequisites and compiled OAP, you can follow the corresponding feature documents for how to use these features.

* [OAP User Guide](../README.md#user-guide)
