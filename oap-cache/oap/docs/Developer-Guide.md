# Developer Guide

* [Build](#Build)
* [Integrate with Spark\*](#integrate-with-spark)
* [Enable NUMA binding for Intel® Optane™ DC Persistent Memory in Spark](#enable-numa-binding-for-dcpmm-in-spark)

## Build

#### Build

Build with using [Apache Maven\*](http://maven.apache.org/).

Clone the OAP project:

```
git clone -b branch-0.8-spark-2.4.x  https://github.com/Intel-bigdata/OAP.git
cd OAP
```

Build the package:

```
mvn clean -pl com.intel.oap:oap-cache -am package
```

#### Run Tests

Run all the tests:
```
mvn clean -pl com.intel.oap:oap-cache -am test
```
Run a specific test suite, for example `OapDDLSuite`:
```
mvn -DwildcardSuites=org.apache.spark.sql.execution.datasources.oap.OapDDLSuite test
```
**NOTE**: Log level of unit tests currently default to ERROR, please override oap-cache/oap/src/test/resources/log4j.properties if needed.

#### Build with Intel® Optane™ DC Persistent Memory Module

Follow these steps:

##### Prerequisites for building with DCPMM support

Install the required packages on the build system:

- gcc-c++
- [cmake](https://help.directadmin.com/item.php?id=494)
- [Memkind](https://github.com/Intel-bigdata/memkind)
- [vmemcache](https://github.com/pmem/vmemcache)


##### Build the package
You need to add -Ppersistent-memory to the build command line for building with DCPMM support. For Non-evictable cache strategy, you need to build with -Ppersistent-memory also.
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

## Integrate with Spark

Although SQL Index and Data Source Cache act as a plug-in JAR to Spark, there are still a few tricks to note when integrating with Spark. The OAP team explored using the Spark extension & data source API to deliver its core functionality. However, the limits of the Spark extension and data source API meant that we had to make some changes to Spark internals. As a result you must check whether your installation is an unmodified Community Spark or a customized Spark.

#### Integrate with Community Spark

If you are running a Community Spark, things will be much simpler. Refer to [User Guide](User-Guide.md) to configure and setup Spark to work with OAP.

#### Integrate with customized Spark

In this case check whether the OAP changes to Spark internals will conflict with or override your private changes. 

- If there are no conflicts or overrides, the steps are the same as the steps of unmodified version of Spark described above. 
- If there are conflicts or overrides, develop a merge plan of the source code to make sure the code changes you made in to the Spark source appear in the corresponding file included in OAP the project. Once merged, rebuild OAP.

The following files need to be checked/compared for changes:

```
•	org/apache/spark/scheduler/DAGScheduler.scala           
		Add the oap cache location to aware task scheduling.
•	org/apache/spark/sql/execution/DataSourceScanExec.scala   
		Add the metrics info to OapMetricsManager and schedule the task to read from the cached 
•	org/apache/spark/sql/execution/datasources/FileFormatDataWriter.scala
                Return the result of write task to driver.
•	org/apache/spark/sql/execution/datasources/FileFormatWriter.scala
		Add the result of write task. 
•	org/apache/spark/sql/execution/datasources/OutputWriter.scala  
		Add new API to support return the result of write task to driver.
•	org/apache/spark/status/api/v1/OneApplicationResource.scala    
		Update the metric data to spark web UI.
•	org/apache/spark/SparkEnv.scala
		Add OapRuntime.stop() to stop OapRuntime instance.
•	org/apache/spark/sql/execution/datasources/parquet/VectorizedColumnReader.java
		Change the private access of variable to protected
•	org/apache/spark/sql/execution/datasources/parquet/VectorizedPlainValuesReader.java
		Change the private access of variable to protected
•	org/apache/spark/sql/execution/datasources/parquet/VectorizedRleValuesReader.java
		Change the private access of variable to protected
•	org/apache/spark/sql/execution/vectorized/OnHeapColumnVector.java
		Add the get and set method for the changed protected variable.
```

## Enable NUMA binding for DCPMM in Spark

#### Rebuild Spark packages with NUMA binding patch 

When using DCPMM as a cache medium apply the [NUMA](https://www.kernel.org/doc/html/v4.18/vm/numa.html) binding patch [Spark.2.4.4.numa.patch](./Spark.2.4.4.numa.patch) to Spark source code for best performance.

1. Download src for [Spark-2.4.4](https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4.tgz) and clone the src from github.

2. Apply this patch and [rebuild](https://spark.apache.org/docs/latest/building-spark.html) the Spark package.

```
git apply  Spark.2.4.4.numa.patch
```

3. Add these configuration items to the Spark configuration file $SPARK_HOME/conf/spark-defaults.conf to enable NUMA binding.


```
spark.yarn.numa.enabled true 
```
**NOTE**: If you are using a customized Spark, you will need to manually resolve the conflicts.

#### Use pre-built patched Spark packages 

If you think it is cumbersome to apply patches, we have a pre-built Spark [spark-2.4.4-bin-hadoop2.7-intel-oap-0.8.tgz](https://github.com/Intel-bigdata/spark/releases/download/v2.4.4-intel-oap-0.8/spark-2.4.4-bin-hadoop2.7-intel-oap-0.8.tgz) with the patch applied.

\*Other names and brands may be claimed as the property of others.