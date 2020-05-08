# ArrowDataSource for Apache Spark
A Spark DataSouce implementation for reading files into Arrow compatible columnar vectors.

## Note
The development of this library is still in progress. As a result some of the functionality may not be constantly stable for being used in production environments that have not been fully considered in the limited testing capabilities so far.

## Build
### Build and install Intel® optimized Arrow with Datasets Java API

```
// build arrow-cpp
git clone --branch native-sql-engine-clean https://github.com/Intel-bigdata/arrow.git
cd arrow/cpp
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_BOOST_USE_SHARED=ON -DARROW_JNI=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_PROTOBUF=ON -DARROW_DATASET=ON ..
make

// build and install arrow-jvm-library
cd ../../java
mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=../cpp/build/release
```

### Build This Library
```
// build
mvn clean package

// check built jar library
readlink -f target/spark-arrow-datasource-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Get Started
### Add extra class pathes to Spark
To enable ArrowDataSource, the previous built jar `spark-arrow-datasource-0.1.0-SNAPSHOT-jar-with-dependencies.jar` should be added to some of Spark configuration options. Typically they are:

* `spark.driver.extraClassPath`
* `spark.executor.extraClassPath`

For more information about these options, please read the official Spark [documentation](https://spark.apache.org/docs/latest/configuration.html#runtime-environment).

### Run a query (Scala)

```scala
val path = "${PATH_TO_YOUR_PARQUET_FILE}"
val df = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
        .format("arrow")
        .load(path)
df.createOrReplaceTempView("my_temp_view")
spark.sql("SELECT * FROM my_temp_view LIMIT 10").show(10)
```