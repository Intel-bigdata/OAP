# OAP - Optimized Analytics Package for Spark Platform
[![Build Status](https://travis-ci.org/Intel-bigdata/OAP.svg?branch=master)](https://travis-ci.org/Intel-bigdata/OAP)

OAP - Optimized Analytics Package (previously known as Spinach) is designed to accelerate Ad-hoc query. OAP defines a new parquet-like columnar storage data format and offering a fine-grained hierarchical cache mechanism in the unit of “Fiber” in memory. What’s more, OAP has extended the Spark SQL DDL to allow user to define the customized indices based on relation.
## Building
```
mvn -DskipTests package
```
## Prerequisites
You should have [Apache Spark](http://spark.apache.org/) of version 2.1.0 installed in your cluster. Refer to Apache Spark's [documents](http://spark.apache.org/docs/2.1.0/) for details.
## Use OAP with Spark
1. Build OAP, `mvn -DskipTests package` and find `oap-<version>.jar` in `target/`
2. Deploy `oap-<version>.jar` to master machine.
3. Put below configurations to _$SPARK_HOME/conf/spark-defaults.conf_
```
spark.files                      file:///path/to/oap-dir/oap-<version>.jar
spark.executor.extraClassPath      ./oap-<version>.jar
spark.driver.extraClassPath        /path/to/oap-dir/oap-<version>.jar
spark.memory.offHeap.enabled       true
spark.memory.offHeap.size          2g
```
4. Run spark by `bin/spark-sql`, `bin/spark-shell`, `sbin/start-thriftserver` or `bin/pyspark` and try our examples

**NOTE**: For spark standalone mode, you have to put `oap-<version>.jar` to both driver and executor since `spark.files` is not working. Also don't forget to update `extraClassPath`.

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
mvn test
```
If you want to run any specific test suite, for example `OapDDLSuite`, use
```
mvn -DwildcardSuites=org.apache.spark.sql.execution.datasources.oap.OapDDLSuite test
```
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

## Configurations and Performance Tuning

Parquet Support - Enable OAP support for parquet files
* Default: true
* Usage: `sqlContext.conf.setConfString(SQLConf.OAP_PARQUET_ENABLED.key, "false")`

Fiber Cache Size - Total Memory size to cache Fiber. Unit: KB
* Default: `spark.executor.memory * 0.3`
* Usage: `sqlContext.conf.setConfString(SQLConf.OAP_FIBERCACHE_SIZE.key, s"{100 * 1024 * 1024}")`

Full Scan Threshold - If the analysis result is above this threshold, it will go through the whole data file instead of read index data.
* Default: 0.8
* Usage: `sqlContext.conf.setConfString(SQLConf.OAP_FULL_SCAN_THRESHOLD.key, "0.8")`

Row Group Size - Row count for each row group
* Default: 1048576
* Usage1: `sqlContext.conf.setConfString(SQLConf.OAP_ROW_GROUP_SIZE.key, "1025")`
* Usage2: `CREATE TABLE t USING oap OPTIONS ('rowgroup' '1024')`

Compression Codec - Choose compression type for OAP data files.
* Default: GZIP
* Values: UNCOMPRESSED, SNAPPY, GZIP, LZO
* Usage1: `sqlContext.conf.setConfString(SQLConf.OAP_COMPRESSION.key, "SNAPPY")`
* Usage2: `CREATE TABLE t USING oap OPTIONS ('compression' 'SNAPPY')`

Refer to [OAP User guide](https://github.com/Intel-bigdata/OAP/wiki/OAP-User-guide) for more details.

## How to Contribute
If you are looking for some ideas on what to contribute, check out GitHub issues for this project labeled ["Pick me up!"](https://github.com/Intel-bigdata/OAP/issues?labels=pick+me+up%21&state=open).
Comment on the issue with your questions and ideas.

We tend to do fairly close readings of pull requests, and you may get a lot of comments. Some common issues that are not code structure related, but still important:
* Please make sure to add the license headers to all new files. You can do this automatically by using the `mvn license:format` command.
* Use 2 spaces for whitespace. Not tabs, not 4 spaces. The number of the spacing shall be 2.
* Give your operators some room. Not `a+b` but `a + b` and not `def foo(a:Int,b:Int):Int` but `def foo(a: Int, b: Int): Int`.
* Generally speaking, stick to the [Scala Style Guide](http://docs.scala-lang.org/style/)
* Make sure tests pass!

