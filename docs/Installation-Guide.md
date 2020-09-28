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

### Installation prerequisites 

Dependencies below are required by OAP, you must compile and install them on each cluster node. We also provide shell scripts to help quickly compile and install all these libraries.

- [Cmake](https://help.directadmin.com/item.php?id=494)
- [Memkind](https://github.com/memkind/memkind/tree/v1.10.1-rc2)
- [Vmemcache](https://github.com/pmem/vmemcache)
- [HPNL](https://github.com/Intel-bigdata/HPNL)
- [PMDK](https://github.com/pmem/pmdk)  
- [GCC > 7](https://gcc.gnu.org/wiki/InstallingGCC)  
- **Requirements for Shuffle Remote PMem Extension**  
If enable Shuffle Remote PMem extension with RDMA, you can refer to [Shuffle Remote PMem Extension Guide](../oap-shuffle/RPMem-shuffle/README.md) to configure and validate RDMA in advance.  

Clone the OAP to your local directory:

```
git clone -b <tag-version>  https://github.com/Intel-bigdata/OAP.git
cd OAP
```

**Note**: The following prepare process needs to run as the ***root*** user.   
We provide scripts to help automatically install all dependencies except RDMA, run:
```shell script
sh $OAP_HOME/dev/install-oap-dependencies.sh
```
If you want to use Shuffle Remote PMem Extension feature with RDMA and have completed the RDMA configuring and validating steps, execute the following commands to run the preparing process:
```shell script
sh $OAP_HOME/dev/install-oap-dependencies.sh --with-rdma
```

Some functions to install prerequisites for OAP have been integrated into this `prepare_oap_env.sh`, you can use options like `prepare_cmake` to install the specified dependencies. Use the following command to learn more.  
```shell script
sh $OAP_HOME/dev/scripts/prepare_oap_env.sh --help
```
If there are any problems during the above preparing process, we recommend you refer to the library documentation listed above, and install it by yourself.

***NOTE:*** If you use `install-oap-dependencies.sh` or `prepare_oap_env.sh` to install GCC, or your GCC is not installed in the default path, please ensure you have exported `CC` (and `CXX`) before calling maven.
```shell script
export CXX=$OAPHOME/dev/thirdparty/gcc7/bin/g++
export CC=$OAPHOME/dev/thirdparty/gcc7/bin/gcc
```


## Compiling OAP
If you have installed all prerequisites, you can download our pre-built package [oap-0.8.2-bin-spark-2.4.4.tar.gz](https://github.com/Intel-bigdata/OAP/releases/download/v0.8.2-spark-2.4.4/oap-0.8.2-bin-spark-2.4.4.tar.gz)  to your working node, unzip it and put the jars to your working directory such as `/home/oap/jars/`, and put the `oap-common-0.8.2-with-spark-2.4.4.jar` to the directory `$SPARK_HOME/jars/`. If you’d like to build from source code,  you can use make-distribution.sh to generate all jars under the dictionary ./dev/release-package in OAP home.
```shell script
sh ./dev/compile-oap.sh
``````

If you only want to build specified OAP module, you can use the command like the following, and then you will find the jars under the module's `target` directory.
```shell script
# build SQL Index & Data Source Cache module
sh $OAPHOME/dev/compile-oap.sh --oap-cache
#or
mvn clean -pl com.intel.oap:oap-cache -am package 
```

```shell script
# build Shuffle Remote PMem Extension module
sh $OAPHOME/dev/compile-oap.sh --oap-rpmem-shuffle
#or
mvn clean -pl com.intel.oap:oap-rpmem-shuffle -am package 
```

```shell script
# build RDD Cache PMem Extension module
sh $OAPHOME/dev/compile-oap.sh --oap-spark
#or
mvn clean -pl com.intel.oap:oap-spark -am package 
```

```shell script
# build Remote Shuffle module
sh $OAPHOME/dev/compile-oap.sh --remote-shuffle
#or
mvn clean -pl com.intel.oap:oap-remote-shuffle -am package 
```

##  Configuration
After you have successfully completed the installation of prerequisites and compiled OAP, you can follow the corresponding feature documents on how to use these features.

* [OAP User Guide](../README.md#user-guide)
