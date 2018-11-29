# OAP - Optimized Analytics Package for Spark Platform
[![Build Status](https://travis-ci.org/Intel-bigdata/OAP.svg?branch=master)](https://travis-ci.org/Intel-bigdata/OAP)

OAP - Optimized Analytics Package (previously known as Spinach) is designed to accelerate Ad-hoc query. OAP defines a new parquet-like columnar storage data format and offering a fine-grained hierarchical cache mechanism in the unit of “Fiber” in memory. What’s more, OAP has extended the Spark SQL DDL to allow user to define the customized indices based on relation.
## Building
By defaut, it builds for Spark 2.1.0. To specify the Spark version, please use profile spark-2.1 , spark-2.2 or spark-2.3.
```
mvn clean -q -Ppersistent-memory -DskipTests package
mvn clean -q -Pspark-2.2 -Ppersistent-memory -DskipTests package
mvn clean -q -Pspark-2.3 -Ppersistent-memory -DskipTests package
```
## Prerequisites
You should have [Apache Spark](http://spark.apache.org/) of version 2.1.0, 2.2.0 or 2.3.0 installed in your cluster. Refer to Apache Spark's [documents](http://spark.apache.org/docs/2.1.0/) for details.
## Use OAP with Spark
1. Build OAP find `oap-<version>-with-<spark-version>.jar` in `target/`
2. Deploy `oap-<version>-with-<spark-version>.jar` to master machine.
3. Put below configurations to _$SPARK_HOME/conf/spark-defaults.conf_
```
spark.files                         file:///path/to/oap-dir/oap-<version>-with-<spark-version>.jar
spark.executor.extraClassPath       ./oap-<version>-with-<spark-version>.jar
spark.driver.extraClassPath         /path/to/oap-dir/oap-<version>-with-<spark-version>.jar
spark.memory.offHeap.enabled        true
spark.memory.offHeap.size           20g
```
4. Run spark by `bin/spark-sql`, `bin/spark-shell`, `sbin/start-thriftserver` or `bin/pyspark` and try our examples

**NOTE**: 1. For spark standalone mode, you have to put `oap-<version>-with-<spark-version>.jar` to both driver and executor since `spark.files` is not working. Also don't forget to update `extraClassPath`.
          2. For yarn mode, we need to config all spark.driver.memory, spark.memory.offHeap.size and spark.yarn.executor.memoryOverhead (should be close to offHeap.size) to enable fiber cache.
          3. The comprehensive guidence and example of OAP configuration can be referred @https://github.com/Intel-bigdata/OAP/wiki/OAP-User-guide. Briefly speaking, the recommanded configuration is one executor per one node with fully memory/computation capability.

## Example
```
./bin/spark-shell
> spark.sql(s"""CREATE TEMPORARY TABLE oap_test (a INT, b STRING)
           | USING oap
           | OPTIONS (path 'hdfs:///oap-data-dir/')""".stripMargin)
> val data = (1 to 300).map { i => (i, s"this is test $i") }.toDF().createOrReplaceTempView("t")
> spark.sql("insert overwrite table oap_test select * from t")
> spark.sql("create oindex index1 on oap_test (a)")
> spark.sql("show oindex from oap_test").show()
> spark.sql("SELECT * FROM oap_test WHERE a = 1").show()
> spark.sql("drop oindex index1 on oap_test")
```
For a more detailed examples with performance compare, you can refer to [this page](https://github.com/Intel-bigdata/OAP/wiki/OAP-examples) for further instructions.
## Running Test

To run all the tests, use
```
mvn clean -q -Pspark-2.3 -Ppersistent-memory test
```
To run any specific test suite, for example `OapDDLSuite`, use
```
mvn -DwildcardSuites=org.apache.spark.sql.execution.datasources.oap.OapDDLSuite test
```
To run test suites using `LocalClusterMode`, please refer to `SharedOapLocalClusterContext`

NOTE: Log level of OAP unit tests currently default to ERROR, please override src/test/resources/log4j.properties if needed.

## Features

* Index - BTREE, BITMAP
Index is an optimization that is widely used in traditional databases. We are adopting 2 most used index types in OAP project.
BTREE index(default in 0.2.0) is intended for datasets that has a lot of distinct values, and distributed randomly, such as telephone number or ID number.
BitMap index is intended for datasets with a limited total amount of distinct values, such as state or age.
* Statistics - MinMax, Bloom Filter
Sometimes, reading index could bring extra cost for some queries, for example if we have to read all the data after all since there's no valid filter. OAP will automatically write some statistic data to index files, depending on what type of index you are using. With statistics, we can make sure we only use index if we can possibly boost the execution.
* Fine-grained cache
OAP format data file consists of several row groups. For each row group, we have many different columns according to user defined table schema. Each column data in one row is called a "Fiber", we are using this as the minimum cache unit.
* Parquet Data Adaptor
Parquet is the most popular and recommended data format in Spark open-source community. Since a lot of potential users are now using Parquet storing their data, it would be expensive for them to shift their existing data to OAP. So we designed the compatible layer to allow user to create index directly on top of parquet data. With the Parquet reader we implemented, query over indexed Parquet data is also accelerated, though not as much as OAP.
* Orc Data Adaptor
Orc is another popular data format. We also designed the compatible layer to allow user to create index directly on top of Orc data. With the Orc reader we implemented, query over indexed Orc data is also accelerated.
* Compatible with multiple versions of spark
OAP is currently compatible with spark2.1, spark2.2 or spark 2.3.

## Configurations and Performance Tuning

Parquet Support - Enable OAP support for parquet files
* Default: true
* Usage: `sqlContext.conf.setConfString(SQLConf.OAP_PARQUET_ENABLED.key, "false")`

Index Directory Setting - Enable OAP support to separate the index file in specific directory. The index file is in the directory of data file in default.
* Default: ""
* Usage1: `sqlContext.conf.setConfString(SQLConf.OAP_INDEX_DIRECTORY.key, "/tmp")`
* Usage2: `SET spark.sql.oap.index.directory = /tmp`

Fiber Cache Size - Total Memory size to cache Fiber, configured implicitly by 'spark.memory.offHeap.size'
* Default Size: `spark.memory.offHeap.size * 0.7`
* Usage: Fiber cache locates in off heap storage memory, basically this size is spark.memory.offHeap.size * 0.7. But as execution can borrow a few memory from storage in UnifiedMemoryManager mode, it may vary during execution.

Full Scan Threshold - If the analysis result is above this threshold, it will go through the whole data file instead of read index data.
* Default: 0.8
* Usage: `sqlContext.conf.setConfString(SQLConf.OAP_FULL_SCAN_THRESHOLD.key, "0.8")`

Row Group Size - Row count for each row group
* Default: 1048576
* Usage1: `sqlContext.conf.setConfString(SQLConf.OAP_ROW_GROUP_SIZE.key, "1048576")`
* Usage2: `CREATE TABLE t USING oap OPTIONS ('rowgroup' '1048576')`

Compression Codec - Choose compression type for OAP data files.
* Default: GZIP
* Values: UNCOMPRESSED, SNAPPY, GZIP, LZO (Note that ORC does not support GZIP)
* Usage1: `sqlContext.conf.setConfString(SQLConf.OAP_COMPRESSION.key, "SNAPPY")`
* Usage2: `CREATE TABLE t USING oap OPTIONS ('compression' 'SNAPPY')`

Refer to [OAP User guide](https://github.com/Intel-bigdata/OAP/wiki/OAP-User-guide) for more details.

## Query Example and Performance Data
Take 2 simple ad-hoc queries as instances, the store_sales table comes from TPCDS with data scale 200G. Generally we can see 5x boost in performance.
1. "SELECT * FROM store_sales WHERE ss_ticket_number BETWEEN 100 AND 200"

Q6:                   | T1/ms | T2/ms | T3/ms | Median/ms 
--------------------- | ----- | ----- | ----- | ---------
oap-with-index        |   542 |   295 |   370 |      370  
parquet-with-index    |  1161 |   682 |   680 |      682  
parquet-without-index |  2010 |  1922 |  1915 |     1922  

2. "SELECT * FROM store_sales WHERE ss_ticket_number < 10000 AND ss_net_paid BETWEEN 100.0 AND 110.0")

Q12:                  | T1/ms | T2/ms | T3/ms | Median/ms 
--------------------- | ----- | ----- | ----- | ---------
oap-with-index        |    509|   431 |   437 |      437
parquet-with-index    |    944|   930 |  1318 |      944
parquet-without-index |   2084|  1895 |  2007 |     2007

## How to Contribute
If you are looking for some ideas on what to contribute, check out GitHub issues for this project labeled ["Pick me up!"](https://github.com/Intel-bigdata/OAP/issues?labels=pick+me+up%21&state=open).
Comment on the issue with your questions and ideas.

We tend to do fairly close readings of pull requests, and you may get a lot of comments. Some common issues that are not code structure related, but still important:
* Please make sure to add the license headers to all new files. You can do this automatically by using the `mvn license:format` command.
* Use 2 spaces for whitespace. Not tabs, not 4 spaces. The number of the spacing shall be 2.
* Give your operators some room. Not `a+b` but `a + b` and not `def foo(a:Int,b:Int):Int` but `def foo(a: Int, b: Int): Int`.
* Generally speaking, stick to the [Scala Style Guide](http://docs.scala-lang.org/style/)
* Make sure tests pass!

