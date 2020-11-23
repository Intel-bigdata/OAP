# Developer Guide

This document is a supplement to the whole [Developer Guide](../../../docs/Developer-Guide.md) for SQL Index and Data Source Cache.
After following that document, you can continue more details for SQL Index and Data Source Cache.

* [Building](#Building)
* [Enabling NUMA binding for Intel® Optane™ DC Persistent Memory in Spark](#enabling-numa-binding-for-pmem-in-spark)

## Building

### Building SQL Index and  Data Source Cache 

Building with [Apache Maven\*](http://maven.apache.org/).

Clone the OAP project:

```
git clone -b <tag-version>  https://github.com/Intel-bigdata/OAP.git
cd OAP
```

Build the `oap-cache` package:

```
mvn clean -pl com.intel.oap:oap-cache -am package
```

### Running Tests

Run all the tests:
```
mvn clean -pl com.intel.oap:oap-cache -am test
```
Run a specific test suite, for example `OapDDLSuite`:
```
mvn -pl com.intel.oap:oap-cache -am -DwildcardSuites=org.apache.spark.sql.execution.datasources.oap.OapDDLSuite test
```
**NOTE**: Log level of unit tests currently default to ERROR, please override oap-cache/oap/src/test/resources/log4j.properties if needed.

### Building with Intel® Optane™ DC Persistent Memory Module

#### Prerequisites for building with PMem support

Install the required packages on the build system:

- [cmake](https://help.directadmin.com/item.php?id=494)
- [memkind](https://github.com/memkind/memkind/tree/v1.10.1)
- [vmemcache](https://github.com/pmem/vmemcache)
- [Plasma](http://arrow.apache.org/blog/2017/08/08/plasma-in-memory-object-store/)

####  memkind installation

The memkind library depends on `libnuma` at the runtime, so it must already exist in the worker node system. Build the latest memkind lib from source:

```
git clone -b v1.10.1 https://github.com/memkind/memkind
cd memkind
./autogen.sh
./configure
make
make install
``` 
#### vmemcache installation

To build vmemcache library from source, you can (for RPM-based linux as example):
```
git clone https://github.com/pmem/vmemcache
cd vmemcache
mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DCPACK_GENERATOR=rpm
make package
sudo rpm -i libvmemcache*.rpm
```
#### Plasma installation

To use optimized Plasma cache with OAP, you need following components:  

   (1) `libarrow.so`, `libplasma.so`, `libplasma_jni.so`: dynamic libraries, will be used in Plasma client.   
   (2) `plasma-store-server`: executable file, Plasma cache service.  
   (3) `arrow-plasma-0.17.0.jar`: will be used when compile oap and spark runtime also need it. 

- `.so` file and binary file  
  Clone code from Intel-arrow repo and run following commands, this will install `libplasma.so`, `libarrow.so`, `libplasma_jni.so` and `plasma-store-server` to your system path(`/usr/lib64` by default). And if you are using Spark in a cluster environment, you can copy these files to all nodes in your cluster if the OS or distribution are same, otherwise, you need compile it on each node.
  
```
cd /tmp
git clone https://github.com/Intel-bigdata/arrow.git
cd arrow && git checkout branch-0.17.0-oap-1.0
cd cpp
mkdir release
cd release
#build libarrow, libplasma, libplasma_java
cmake -DCMAKE_INSTALL_PREFIX=/usr/ -DCMAKE_BUILD_TYPE=Release -DARROW_BUILD_TESTS=on -DARROW_PLASMA_JAVA_CLIENT=on -DARROW_PLASMA=on -DARROW_DEPENDENCY_SOURCE=BUNDLED  ..
make -j$(nproc)
sudo make install -j$(nproc)
```

- arrow-plasma-0.17.0.jar  
   `arrow-plasma-0.17.0.jar` is provided in Maven central repo, you can download [it](https://repo1.maven.org/maven2/com/intel/arrow/arrow-plasma/0.17.0/arrow-plasma-0.17.0.jar) and copy to `$SPARK_HOME/jars` dir.
   Or you can manually install it, change to arrow repo java direction, run following command, this will install arrow jars to your local maven repo, and you can compile oap-cache module package now. Besides, you need copy arrow-plasma-0.17.0.jar to `$SPARK_HOME/jars/` dir, cause this jar is needed when using external cache.
   
```
cd $ARROW_REPO_DIR/java
mvn clean -q -pl plasma -DskipTests install
```


#### Building the package
You need to add `-Ppersistent-memory` to build with PMem support. For `noevict` cache strategy, you also need to build with `-Ppersistent-memory` parameter.
```
mvn clean -q -pl com.intel.oap:oap-cache -am  -Ppersistent-memory -DskipTests package
```
For vmemcache cache strategy, please build with command:
```
mvn clean -q -pl com.intel.oap:oap-cache -am -Pvmemcache -DskipTests package
```
Build with this command to use all of them:
```
mvn clean -q -pl com.intel.oap:oap-cache -am  -Ppersistent-memory -Pvmemcache -DskipTests package
```

## Enabling NUMA binding for PMem in Spark

### Rebuilding Spark packages with NUMA binding patch 

When using PMem as a cache medium apply the [NUMA](https://www.kernel.org/doc/html/v4.18/vm/numa.html) binding patch [numa-binding-spark-3.0.0.patch](./numa-binding-spark-3.0.0.patch) to Spark source code for best performance.

1. Download src for [Spark-3.0.0](https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0.tgz) and clone the src from github.

2. Apply this patch and [rebuild](https://spark.apache.org/docs/latest/building-spark.html) the Spark package.

```
git apply  numa-binding-spark-3.0.0.patch
```

3. Add these configuration items to the Spark configuration file $SPARK_HOME/conf/spark-defaults.conf to enable NUMA binding.


```
spark.yarn.numa.enabled true 
```
**NOTE**: If you are using a customized Spark, you will need to manually resolve the conflicts.

###### \*Other names and brands may be claimed as the property of others.
