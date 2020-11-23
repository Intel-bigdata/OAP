# OAP Developer Guide

This document contains the instructions & scripts on installing necessary dependencies and building OAP. 
You can get more detailed information from OAP each module below.

* [SQL Index and Data Source Cache](../oap-cache/oap/docs/Developer-Guide.md)
* [RDD Cache PMem Extension](../oap-spark/README.md#compiling)
* [Shuffle Remote PMem Extension](../oap-shuffle/RPMem-shuffle/README.md#5-install-dependencies-for-shuffle-remote-pmem-extension)
* [Remote Shuffle](../oap-shuffle/remote-shuffle/README.md#build-and-deploy)
* [Intel MLlib](../oap-mllib/README.md)
* [Unified Arrow Data Source](../oap-data-source/arrow/README.md)
* [Native SQL Engine](../oap-native-sql/README.md)

## Building OAP

### Prerequisites for Building

OAP is built with [Apache Maven](http://maven.apache.org/) and Oracle Java 8, and mainly required tools to install on your cluster are listed below.

- [Cmake](https://help.directadmin.com/item.php?id=494)
- [GCC > 7](https://gcc.gnu.org/wiki/InstallingGCC)
- [Memkind](https://github.com/memkind/memkind/tree/v1.10.1-rc2)
- [Vmemcache](https://github.com/pmem/vmemcache)
- [HPNL](https://github.com/Intel-bigdata/HPNL)
- [PMDK](https://github.com/pmem/pmdk)  
- [OneAPI](https://software.intel.com/content/www/us/en/develop/tools/oneapi.html)
- [Arrow](https://github.com/Intel-bigdata/arrow)

- **Requirements for Shuffle Remote PMem Extension**  
If enable Shuffle Remote PMem extension with RDMA, you can refer to [Shuffle Remote PMem Extension Guide](../oap-shuffle/RPMem-shuffle/README.md) to configure and validate RDMA in advance.

We provide scripts below to help automatically install dependencies above **except RDMA**, need change to **root** account, run:

```
# git clone -b <tag-version> https://github.com/Intel-bigdata/OAP.git
# cd OAP
# sh $OAP_HOME/dev/install-compile-time-dependencies.sh
```

Run the following command to learn more.

```
# sh $OAP_HOME/dev/scripts/prepare_oap_env.sh --help
```

Run the following command to automatically install specific dependency such as Maven.

```
# sh $OAP_HOME/dev/scripts/prepare_oap_env.sh --prepare_maven
```


### Building

To build OAP package, run command below then you can find a tarball named `oap-$VERSION-bin-spark-$VERSION.tar.gz` under directory `$OAP_HOME/dev/release-package `.
```
$ sh $OAP_HOME/dev/compile-oap.sh
```

Building Specified OAP Module, such as `oap-cache`, run:
```
$ sh $OAP_HOME/dev/compile-oap.sh --oap-cache
```


### Running OAP Unit Tests

Setup building environment manually for intel MLlib and export `CC` & `CXX` before using `mvn`, run

```
$ export CXX=$OAP_HOME/dev/thirdparty/gcc7/bin/g++
$ export CC=$OAP_HOME/dev/thirdparty/gcc7/bin/gcc
$ export ONEAPI_ROOT=/opt/intel/inteloneapi
$ source /opt/intel/inteloneapi/daal/2021.1-beta07/env/vars.sh
$ source /opt/intel/inteloneapi/tbb/2021.1-beta07/env/vars.sh
$ source /tmp/oneCCL/build/_install/env/setvars.sh
```

Run all the tests:

```
$ mvn clean test
```

Run Specified OAP Module Unit Test, such as `oap-cache`:

```
$ mvn clean -pl com.intel.oap:oap-cache -am test

```

### Building SQL Index and Data Source Cache with PMem

#### Prerequisites for building with PMem support

When using SQL Index and Data Source Cache with PMem, finish steps of [Prerequisites for building](#Prerequisites-for-building) to ensure needed dependencies have been installed.

#### Building package

You can build OAP with PMem support with command below:

```
$ sh $OAP_HOME/dev/compile-oap.sh
```
Or run:

```
$ mvn clean -q -Ppersistent-memory -Pvmemcache -DskipTests package
```

## Contributing

This session introduces what is required before submitting a code change to OAP.

- We continue to use the Github **Issues** to track the new features/tasks/issues.​

- For every commit, we need an issue id for the commit. ​

- Format the log message as following: **[OAP-IssuesId][optional:ModuleName] detailed message**​ 

  like [OAP-1406][rpmem-shuffle]Add shuffle block removing operation within one Spark context 

- Always merge your pull request as a single commit and the commit message follow the above format.​

- The formal features names in 0.9 are: **SQL Index**, **SQL Data Source Cache**, **Native SQL Engine**, **Unified Arrow Data Source**, **RDD Cache PMem Extension**, **RPMem Shuffle**, **Remote Shuffle**, **Intel MLlib**.

We don’t strictly request the module id the same as the feature name. Please align in the feature members to use a consistent name in the log message.​
