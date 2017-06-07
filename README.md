# OAP - Optimized Analytics Package for Spark Platform
[![Build Status](https://travis-ci.org/Intel-bigdata/OAP.svg?branch=master)](https://travis-ci.org/Intel-bigdata/OAP)

OAP - Optimized Analytics Package (Spinach as code name) is designed to accelerate Ad-hoc query. OAP defines a new parquet-like columnar storage data format and offering a fine-grained hierarchical cache mechanism in the unit of “Fiber” in memory. What’s more, OAP has extended the Spark SQL DDL to allow user to define the customized indices based on relation.
## Building
```
mvn -DskipTests package
```
## Use OAP with Spark
After `mvn package` you will find `oap-<version>.jar` in `target/`. Update `spark.driver.extraClassPath` and `spark.executor.extraClassPath` to include this jar file, and you can use Spinach from `bin/spark-sql`, `bin/spark-shell` or `sbin/start-thriftserver` as you usually do.
## Example
```
./bin/spark-shell
> spark.sql(s"""CREATE TEMPORARY TABLE oap_test (a INT, b STRING)
           | USING spn
           | OPTIONS (path 'hdfs:///oap-data-dir/')""".stripMargin)
> val data = (1 to 300).map { i => (i, s"this is test $i") }.toDF().createOrReplaceTempView("t")
> spark.sql("insert overwrite table oap_test select * from t")
> spark.sql("create sindex index1 on oap_test (a)")
> spark.sql("show sindex from oap_test")
> spark.sql("SELECT * FROM oap_test WHERE a = 1").show()
> spark.sql("drop sindex index on oap_test")
```
## Running Test
```
mvn test
```
## Features
* Index - BTREE, BITMAP
* Statistics - MinMax, Bloom Filter
* Fine-grained cache
* Parquet Data Adaptor
## Configuration
spinach.rowgroup.szie
* Row count for each row group
* Default: 1024
* Usage: `System.setProperty("spinach.rowgroup.size", "1024")`

spinach.compression.codec
* Choose compression type
* Default: GZIP
* Values: UNCOMPRESSED, SNAPPY, GZIP, LZO
* Usage: `System.setProperty("spinach.compression.codec", "SNAPPY")`
## How to Contribute
If you are looking for some ideas on what to contribute, check out GitHub issues for this project labeled ["Pick me up!"](https://github.com/Intel-bigdata/Spinach/issues?labels=pick+me+up%21&state=open).
Comment on the issue with your questions and ideas.

We tend to do fairly close readings of pull requests, and you may get a lot of comments. Some common issues that are not code structure related, but still important:
* Please make sure to add the license headers to all new files. You can do this automatically by using the `mvn license:format` command.
* Use 2 spaces for whitespace. Not tabs, not 4 spaces. The number of the spacing shall be 2.
* Give your operators some room. Not `a+b` but `a + b` and not `foo(int a,int b)` but `foo(int a, int b)`.
* Generally speaking, stick to the [Scala Style Code](http://docs.scala-lang.org/style/)
* Make sure tests pass!

