# OAP Product Developer Guide

**NOTE**: This document is for the whole project of OAP about building, you can learn more detailed information from every module's  document.
* [OAP Project Cache](../oap-cache/oap/docs/Developer-Guide.md) 
* [OAP Project Remote Shuffle](../oap-shuffle/remote-shuffle/README.md) 

## OAP Product Building

To clone OAP project, use
```shell script
git clone -b branch-0.8-spark-2.4.x  https://github.com/Intel-bigdata/OAP.git
cd OAP
```

#### Prerequisites for Building
OAP is built using [Apache Maven](http://maven.apache.org/). You need to install the required packages on the build system listed below.
- [Cmake](https://help.directadmin.com/item.php?id=494)
- [Memkind](https://github.com/Intel-bigdata/memkind)
- [Vmemcache](https://github.com/pmem/vmemcache)

You can use the following command under the folder dev to automatically install these dependencies.

```shell script
source $OAP_HOME/dev/prepare_oap_env.sh
prepare_maven
```
You can also use command like prepare_cmake to install the specified dependencies after executing the command "source prepare_oap_env.sh". Use the following command to learn more.

```shell script
oap_build_help
```

If you want to automatically install all dependencies,use:
```shell script
prepare_all
```

#### Building


To build OAP package, use
```shell script
mvn clean -DskipTests package
```

#### Building Specified Module
```shell script
mvn clean -pl com.intel.oap:oap-cache -am package
```

#### Running Test

To run all the tests, use
```shell script
mvn clean test
```

#### Running Specified Test
```shell script
mvn clean -pl com.intel.oap:oap-cache -am test
```

#### OAP Building with DCPMM

##### Prerequisites for building with DCPMM support

If you want to use OAP-CACHE with DCPMM,  you must finish steps of "Prerequisites for building" to ensure all dependencies have been installed .

##### Building package
You need to add -Ppersistent-memory to the build command line for building with DCPMM support. For Non-evictable cache stratege, you need to build with -Ppersistent-memory also.
```shell script
mvn clean -q -Ppersistent-memory -DskipTests package
```
for vmemcache cache strategy, please build with command:
```shell script
mvn clean -q -Pvmemcache -DskipTests package
```
You can build with command to use all of them:
```shell script
mvn clean -q -Ppersistent-memory -Pvmemcache -DskipTests package
```


#### OAP Packaging 
If you want to generate a release package after you mvn package all modules, use the following command under the directory dev, then you can find a tarball named oap-product-0.8.0-bin-spark-2.4.4.tar.gz in dev/release-package dictionary.
```shell script
sh make-distribution.sh
```