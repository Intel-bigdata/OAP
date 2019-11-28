
# Spark OAP Integration Guide

## Background 
Although OAP (Optimized Analytical Package for Spark) acts as a plugin jar to Spark, there are still a few tricks to note when integration with Spark. The document gives you an overview of areas to check when integrating OAP with your Spark. Basically, OAP explored Spark extension & data source API to perform its core functionality. But there are other functionality aspects that cannot achieved by Spark extension and data source API. We made a few improvements or changes to the Spark internals to achieve the functionality.
When you are doing an integration, you need to check whether you are running an unmodified up-stream version of Spark or a modified customized version of Spark.

## Integration an unmodified up-stream version of Spark
If you are running an unmodified up-stream version of Spark, things will be much simple. We currently support Spark Spark 2.3.2 & Spark 2.4.1.
In this case, you can follow the below steps to integrate OAP to Spark.

### Building
Building the OAP version for the specific Spark version. For example, the following builds OAP 0.6 for Spark 2.3.2
```
git checkout -b branch-0.6-spark-2.3.2 origin/branch-0.6-spark-2.3.2
mvn clean -q -Ppersistent-memory -DskipTests package. Profile persistent-memory is Optional.
```
### Deploy
1.	Spark on Yarn with Client Mode
In this mode, you need set the following three configurations in the “$SPARK_HOME/conf/sparkdefaults.conf” file. And then you can run by bin/spark-sql, bin/spark-shell, bin/spark-submit --deploymode client, sbin/start-thriftserver or bin/pyspark.
```
spark.files /{PATH_TO_OAP_JAR}/oap-0.6-with-spark-2.3.2.jar # absolute path 
spark.executor.extraClassPath ./ oap-0.6-with-spark-2.3.2.jar # relative path
spark.driver.extraClassPath /{PATH_TO_OAP_JAR}/ oap-0.6-with-spark-2.3.2.jar # absolute path
```
2. Spark on Yarn with Cluster Mode
In this mode, you need set the following three configurations in “$SPARK_HOME/conf/sparkdefaults.conf” file. And then you can run with bin/spark-submit –deploy-mode cluster.
```
spark.files /{PATH_TO_OAP_JAR}/oap-0.6-with-spark-2.3.2.jar # absolute path 
spark.executor.extraClassPath ./ oap-0.6-with-spark-2.3.2.jar # relative path 
spark.driver.extraClassPath ./oap-0.6-with-spark-2.3.2.jar # relative path
```
### Run

1. Index function
You can run with the following example using spark shell for testing simple index case. And you need to replace the “namenode“ with your own cluster.

```
./bin/spark-shell
> spark.sql(s"""CREATE TEMPORARY TABLE oap_test (a INT, b STRING)
| USING oap)
| OPTIONS (path 'hdfs://${namenode}:9000/path')""".stripMargin)
> val data = (1 to 300).map { i => (i, s"this is test $i") }.toDF().createOrReplaceTempView("t")
> spark.sql("insert overwrite table oap_test select * from t")
> spark.sql("create oindex index1 on oap_test (a)")
> spark.sql("show oindex from oap_test").show()
> spark.sql("SELECT * FROM oap_test WHERE a = 1").show()
> spark.sql("drop oindex index1 on oap_test")
```

2. Cache function
If you want to run OAP with cache function, you need add the following 
configurations in the 
“$SPARK_HOME/conf/spark-defaults.conf” file and then run by sbin/start-thriftserver.sh. After start the 
thriftserver service, you can find the cache metric with OAP tab in the spark history Web UI. OAP provide two medias to cache the hot data: DRAM and DCPMM.

DRAM Cache Configuration in $SPARK_HOME/conf/spark-defaults.conf
```
spark.sql.extensions org.apache.spark.sql.OapExtensions
spark.memory.offHeap.enabled true
spark.memory.offHeap.size
spark.sql.oap.parquet.data.cache.enable true (for parquet fileformat)
spark.sql.oap.orc.data.cache.enable true (for orc fileformat)
```
DCPMM Cache configuration in $SPARK_HOME/conf/spark-defaults.conf
```
spark.sql.extensions org.apache.spark.sql.OapExtensions
spark.sql.oap.fiberCache.memory.manager pm
spark.sql.oap.fiberCache.persistent.memory.initial.size
spark.sql.oap.fiberCache.persistent.memory.reserved.size
spark.sql.oap.parquet.data.cache.enable true (for parquet fileformat)
spark.sql.oap.orc.data.cache.enable true (for orc fileformat)
```
## Integration a modified customized version of Spark
It will be more complicated to integrate OAP with a customized version of Spark. Steps needed for this case is to check whether the OAP changes of Spark internals will conflict or override with your private changes. 
•	If no conflicts or overrides happens, the steps are the same as the steps of unmodified version of Spark described above. 
•	If conflicts or overrides happen, you need to have a merge plan of the source code to make sure the changes you made in a file appears in the corresponding file changed in OAP project. Once merged, the steps are the same as above.

The following file needs to be checked/compared:
```
•	antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4  
Add index related command in this file, such as "create/show/drop oindex". 
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









