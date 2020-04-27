# OAP Developer Guide

* [Build OAP](#build_oap)
* [Integrate with Spark](#integrate-with-spark)
* [Enable NUMA binding for DCPMM in Spark](#enable-numa-binding-for-dcpmm-in-spark)

## Build OAP

OAP is built using [Apache Maven](http://maven.apache.org/).

Clone the OAP current development branch:

```
git clone -b branch-0.7-spark-2.4.x  https://github.com/Intel-bigdata/OAP.git
cd OAP
```

Build the OAP package:

```
mvn clean -DskipTests package
```

### Run Tests

Run all the tests:
```
mvn clean test
```
Run a specific test suite, for example `OapDDLSuite`:
```
mvn -DwildcardSuites=org.apache.spark.sql.execution.datasources.oap.OapDDLSuite test
```
**NOTE**: The default Log level of OAP unit tests is ERROR. Override src/test/resources/log4j.properties if needed.

### Build OAP with DCPMM

#### Prerequisites for building with DCPMM support

Install the required packages on the build system:

- gcc-c++
- [cmake](https://help.directadmin.com/item.php?id=494)
- [Memkind](https://github.com/memkind/memkind)
- [vmemcache](https://github.com/pmem/vmemcache)


#### Build package
Add -Ppersistent-memory to the build command line to build with DCPMM support. 

The Non-evictable cache strategy requires -Ppersistent-memory.

```
mvn clean -q -Ppersistent-memory -DskipTests package
```

The vmemcache cache strategy requires -Pvmemcache:

```
mvn clean -q -Pvmemcache -DskipTests package
```

Add all options:
```
mvn clean -q -Ppersistent-memory -Pvmemcache -DskipTests package
```

## Integrate with Spark

Although OAP acts as a plug-in `.jar` to Spark, there are still a few tricks to note when integrating with Spark. While the OAP project explored using a Spark extension and the data source API to add its core functions, some required functionality could not be achieved this way. As a result, we made some changes to Spark internals instead. Before you begin, check whether you are running an unmodified Community Spark or a customized version.

#### Integrate with Community Spark

If you are running Community Spark, refer to the [OAP user guide](OAP-User-Guide.md) to configure and setup Spark to work with OAP.

#### Integrate with customized Spark

To integrate OAP with a customized installation of Spark, check whether the OAP changes of Spark internals will conflict or override your changes.

- If there are no conflicts or overrides, the steps are the same an unmodified version of Spark described above. 
- If there are conflicts or overrides, carefully merge the source code to make sure the your code changes stay in the corresponding file included in OAP project. Once merged, rebuild OAP.

The following files need to be checked/compared for changes:

```
•	antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4  
		Add index related DDL in this file, such as "create/show/drop oindex". 
•	org/apache/spark/scheduler/DAGScheduler.scala           
		Add the oap cache location to aware task scheduling.
•	org/apache/spark/sql/execution/DataSourceScanExec.scala   
		Add the metrics info to OapMetricsManager and schedule the task to read from the cached hosts.
•	org/apache/spark/sql/execution/datasources/FileFormatWriter.scala
		Return the result of write task to driver.
•	org/apache/spark/sql/execution/datasources/OutputWriter.scala  
		Add new API to support return the result of write task to driver.
•	org/apache/spark/sql/hive/thriftserver/HiveThriftServer2.scala
		Add OapEnv.init() and OapEnv.stop
•	org/apache/spark/sql/hive/thriftserver/SparkSQLCLIDriver.scala
		Add OapEnv.init() and OapEnv.stop in SparkSQLCLIDriver
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

### Rebuild Spark packages with the NUMA binding patch 

When using DCPMM as a cache medium, apply the [NUMA](https://www.kernel.org/doc/html/v4.18/vm/numa.html) [binding patch (Spark.2.4.4.numa.patch)](./Spark.2.4.4.numa.patch) to the Spark source code for optimal performance.

1. Download the source for [Spark-2.4.4](https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4.tgz) and clone the source from GitHub\*.

2. Apply the NUMA patch and [rebuild](https://spark.apache.org/docs/latest/building-spark.html) the Spark package.

```
git apply  Spark.2.4.4.numa.patch
```

3. When deploying OAP to Spark, add this configuration item to the Spark configuration file $SPARK_HOME/conf/spark-defaults.conf to enable NUMA binding.

```
spark.yarn.numa.enabled true 
```
Note: If you are using a customized version of Spark, there may be conflicts in applying the patch. Manually resolve these conflicts.

#### Use pre-built patched Spark packages 


If you think it is cumbersome to apply patches, we have a pre-built Spark [spark-2.4.4-bin-hadoop2.7-patched.tgz](https://github.com/Intel-bigdata/OAP/releases/download/v0.7.0-spark-2.4.4/spark-2.4.4-bin-hadoop2.7-patched.tgz) with the patch applied.


