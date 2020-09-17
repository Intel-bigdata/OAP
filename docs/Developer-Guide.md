# OAP Developer Guide

**NOTE**: This document is for the whole project of OAP about building, you can learn more detailed information from every module's  document.
* [SQL Index and Data Source Cache](../oap-cache/oap/docs/Developer-Guide.md)
* [RDD Cache PMem Extension](../oap-spark/README.md#compiling)
* [Shuffle Remote PMem Extension](../oap-shuffle/RPMem-shuffle/README.md#5-install-dependencies-for-shuffle-remote-pmem-extension)
* [Remote Shuffle](../oap-shuffle/remote-shuffle/README.md#build-and-deploy)
* [Intel MLlib](../oap-mllib/README.md)
* [Unified Arrow Data Source](../oap-data-source/arrow/README.md)
* [Native SQL Engine](../oap-native-sql/README.md)
## OAP Building

To clone OAP project, use
```shell script
git clone -b <tag-version> https://github.com/Intel-bigdata/OAP.git
cd OAP
```

#### Prerequisites for Building
OAP is built using [Apache Maven](http://maven.apache.org/) and Java8. You need to install the required packages on the build system listed below. 
- **Requirements for Shuffle Remote PMem Extension**  
To enable Shuffle Remote PMem extension, you must configure and validate RDMA in advance, you can refer to [Shuffle Remote PMem Extension Guide](../oap-shuffle/RPMem-shuffle/README.md) for more details.
- **Requirements for OAP MLlib**  
To enable OAP MLlib, you need to install oneDAL and oneCLL, they can be downloaded and install from [here](https://software.intel.com/content/www/us/en/develop/tools/oneapi.html). 
- [Cmake](https://help.directadmin.com/item.php?id=494)
- [Memkind](https://github.com/memkind/memkind/tree/v1.10.1-rc2)
- [Vmemcache](https://github.com/pmem/vmemcache)
- [HPNL](https://github.com/Intel-bigdata/HPNL)
- [PMDK](https://github.com/pmem/pmdk)  
- [Arrow](https://github.com/Intel-bigdata/arrow)
- [GCC > 7](https://gcc.gnu.org/wiki/InstallingGCC)

You can use the following command under the folder dev to automatically install these dependencies.

```shell script
sh $OAP_HOME/dev/scripts/prepare_oap_env.sh --prepare_maven

```
You can also use command like prepare_cmake to install the specified dependencies by the script `prepare_oap_env.sh`. Use the following command to learn more.

```shell script
sh $OAP_HOME/dev/scripts/prepare_oap_env.sh --help
```

If you want to automatically install all dependencies,use:
```shell script
sh $OAP_HOME/dev/install-compile-time-dependencies.sh
```

#### Important

If you use `install-compile-time-dependencies.sh` or `prepare_oap_env.sh` to install GCC, or your GCC is not installed in the default path, please ensure you have exported `CC` (and `CXX`) before calling maven.
```shell script
export CXX=$OAPHOME/dev/thirdparty/gcc7/bin/g++
export CC=$OAPHOME/dev/thirdparty/gcc7/bin/gcc
```

#### Building

To build OAP package, use
```shell script
sh $OAPHOME/dev/compile-oap.sh
#or
mvn clean -DskipTests package
```

#### Building Specified Module
```shell script
sh $OAPHOME/dev/compile-oap.sh --oap-cache
#or
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
sh $OAPHOME/dev/compile-oap.sh
```
## Contributing
This session includes what is required before submitting a code change.

- We continue to use the Github **Issues** to track the new features/tasks/issues.​

- For every commit, we need an issue id for the commit. ​

- Format the log message as following: **[OAP-IssuesId][optional:ModuleName] detailed message**​ 

  like [OAP-1406][rpmem-shuffle]Add shuffle block removing operation within one Spark context 

- Always merge your pull request as a single commit and the commit message follow the above format.​

- The formal features names in 0.9 are: **SQL Index**, **SQL Data Source Cache**, **Native SQL Engine**, **Unified Arrow Data Source**, **RDD Cache PMem Extension**, **RPMem Shuffle**, **Remote Shuffle**, **Intel MLlib**.

We don’t strictly request the module id the same as the feature name. Please align in the feature members to use a consistent name in the log message.​
