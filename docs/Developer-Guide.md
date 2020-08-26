# OAP Developer Guide

**NOTE**: This document is for the whole project of OAP about building, you can learn more detailed information from every module's  document.
* [SQL Index and Data Source Cache](../oap-cache/oap/docs/Developer-Guide.md)
* [RDD Cache PMem Extension](../oap-spark/README.md#compiling)
* [Shuffle Remote PMem Extension](../oap-shuffle/RPMem-shuffle/README.md#5-install-dependencies-for-shuffle-remote-pmem-extension)
* [Remote Shuffle](../oap-shuffle/remote-shuffle/README.md#build-and-deploy)

## OAP Building

To clone OAP project, use
```shell script
git clone -b <tag-version> https://github.com/Intel-bigdata/OAP.git
cd OAP
```

#### Prerequisites for Building
OAP is built using [Apache Maven](http://maven.apache.org/). You need to install the required packages on the build system listed below. To enable Shuffle Remote PMem extension, you must configure and validate RDMA in advance, you can refer to [Shuffle Remote PMem Extension Guide](../oap-shuffle/RPMem-shuffle/README.md) for more details.

- [Cmake](https://help.directadmin.com/item.php?id=494)
- [Memkind](https://github.com/memkind/memkind/tree/v1.10.1-rc2)
- [Vmemcache](https://github.com/pmem/vmemcache)
- [HPNL](https://github.com/Intel-bigdata/HPNL)
- [PMDK](https://github.com/pmem/pmdk)  
- [GCC > 7](https://gcc.gnu.org/wiki/InstallingGCC)

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

If you use "prepare_oap_env.sh" to install GCC, or your GCC is not installed in the default path, please export CC (and CXX) before calling maven.
```shell script
export CXX=$OAPHOME/dev/thirdparty/gcc7/bin/g++
export CC=$OAPHOME/dev/thirdparty/gcc7/bin/gcc
```

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

#### OAP Building with PMem

##### Prerequisites for building with PMem support

If you want to use OAP-CACHE with PMem,  you must finish steps of "Prerequisites for building" to ensure all dependencies have been installed .

##### Building package
You need to add `-Ppersistent-memory` to build with PMem support. For `noevict` cache strategy, you also need to build with `-Ppersistent-memory` parameter.
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
If you want to generate a release package after you mvn package all modules, use the following command under the directory dev, then you can find a tarball named oap-product-$VERSION-bin-spark-3.0.0.tar.gz in dev/release-package dictionary.
```shell script
sh make-distribution.sh
```
