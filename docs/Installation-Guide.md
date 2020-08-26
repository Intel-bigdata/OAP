# OAP Installation Guide
This document is to provide you information on how to compile OAP and its dependencies, and install them on your cluster nodes. Some steps include compiling and installing specific libraries to your system, which requires **root** access.

## Contents
  - [Prerequisites](#prerequisites)
      - [Install prerequisites](#install-prerequisites)
  - [Compiling OAP](#compiling-oap)
  - [Configuration](#configuration)

## Prerequisites 

- **OS Requirements**  
We have tested OAP on Fedora 29 and CentOS 7.6 (kernel-4.18.16). We recommend you use **Fedora 29 CentOS 7.6 or above**. Besides, for [Memkind](https://github.com/memkind/memkind/tree/v1.10.1-rc2) we recommend you use **kernel above 3.10**.

- **Requirements for Shuffle Remote PMem Extension**  
If you want to use Shuffle Remote PMem Extension, you need to configure and validate RDMA before these installation steps. You can refer to [Shuffle Remote PMem Extension Guide](../oap-shuffle/RPMem-shuffle/README.md#4-configure-and-validate-rdma) for the details of configuring and validating RDMA.

### Installation prerequisites 

Dependencies below are required by OAP, you must compile and install them on each cluster node. We also provide shell scripts to help quickly compile and install all these libraries.

- [Cmake](https://help.directadmin.com/item.php?id=494)
- [Memkind](https://github.com/memkind/memkind/tree/v1.10.1-rc2)
- [Vmemcache](https://github.com/pmem/vmemcache)
- [HPNL](https://github.com/Intel-bigdata/HPNL)
- [PMDK](https://github.com/pmem/pmdk)  
- [GCC > 7](https://gcc.gnu.org/wiki/InstallingGCC)  

Clone the OAP to your local directory:

```
git clone -b <tag-version>  https://github.com/Intel-bigdata/OAP.git
cd OAP
```

**Note**: The following prepare process needs to run as the ***root*** user. And assume you run the following commands under the OAP home directory.

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
Some functions to install prerequisites for OAP have been integrated into this `prepare_oap_env.sh`, you can use command like `prepare_cmake` to install the specified dependencies after executing the command `source prepare_oap_env.sh`. Use the following command to learn more.  

```shell script
oap_build_help
```
If there are any problems during the above preparing process, we recommend you refer to the library documentation listed above, and install it by yourself.


## Compiling OAP
If you have installed all prerequisites, you can download our pre-built package [oap-0.9.0-bin-spark-3.0.0.tar.gz](https://github.com/Intel-bigdata/OAP/releases/download/v0.9.0-spark-3.0.0/oap-0.9.0-bin-spark-3.0.0.tar.gz)  to your working node, unzip it and put the jars to your working directory such as `/home/oap/jars/`, and put the `oap-common-0.9.0-with-spark-3.0.0.jar` to the directory `$SPARK_HOME/jars/`. If you’d like to build from source code,  you can use make-distribution.sh to generate all jars under the dictionary ./dev/release-package in OAP home.
```shell script
sh ./dev/make-distribution.sh
``````
If you use "prepare_oap_env.sh" to install GCC, or your GCC is not installed in the default path, please export CC (and CXX) before calling maven.
```shell script
export CXX=$OAPHOME/dev/thirdparty/gcc7/bin/g++
export CC=$OAPHOME/dev/thirdparty/gcc7/bin/gcc
```

If you only want to build specified OAP module, you can use the command like the following, and then you will find the jars under the module's `target` directory.
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
After you have successfully completed the installation of prerequisites and compiled OAP, you can follow the corresponding feature documents on how to use these features.

* [OAP User Guide](../README.md#user-guide)
