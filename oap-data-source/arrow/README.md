# ArrowDataSource for Apache Spark
A Spark DataSouce implementation for reading files into Arrow compatible columnar vectors.

## Note
The development of this library is still in progress. As a result some of the functionality may not be constantly stable for being used in production environments that have not been fully considered due to the limited testing capabilities so far.

## Build
### Install libhdfs3 library

For better performance ArrowDataSource reads HDFS files using the third-party library libhdfs3. The library should be pre-installed 
on machines Spark Executor nodes are running on.

To install the library, use of [Conda](https://docs.conda.io/en/latest/) is recommended.

```
// installing libhdfs3
conda install -c conda-forge libhdfs3

// check the installed library file
ls -l ~/miniconda/envs/${YOUR_ENV_NAME}/lib/libhdfs3.so/lib/libhdfs3.so
```

Then add following Spark configuration options before running the DataSource to make the library to be recognized:

* `spark.executorEnv.ARROW_LIBHDFS3_DIR = "~/miniconda/envs/${YOUR_ENV_NAME}/lib/"`
* `spark.executorEnv.LD_LIBRARY_PATH = "~/miniconda/envs/${YOUR_ENV_NAME}/lib/"`

### Build and install Intel® optimized Arrow with Datasets Java API

```
// build arrow-cpp
git clone --branch native-sql-engine-clean https://github.com/Intel-bigdata/arrow.git
cd arrow/cpp
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_BOOST_USE_SHARED=ON -DARROW_JNI=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_PROTOBUF=ON -DARROW_DATASET=ON ..
make

// build and install arrow jvm library
cd ../../java
mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=../cpp/build/release
```

### Build this library

```
// build
mvn clean package

// check built jar library
readlink -f standard/target/spark-arrow-datasource-standard-0.9.0-jar-with-dependencies.jar
```

### Download Spark 3.0.0

Currently ArrowDataSource works on the Spark 3.0.0 version.

```
wget http://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz
tar -xf ./spark-3.0.0-bin-hadoop2.7.tgz
export SPARK_HOME=`pwd`/spark-3.0.0-bin-hadoop2.7
```

If you are new to Apache Spark, please go though [Spark's official deploying guide](https://spark.apache.org/docs/latest/cluster-overview.html) before getting started with ArrowDataSource.

## Get started
### Add extra class pathes to Spark

To enable ArrowDataSource, the previous built jar `spark-arrow-datasource-standard-0.9.0-jar-with-dependencies.jar` should be added to Spark configuration. Typically the options are:

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

## Work together with ParquetDataSource (experimental)

We provide a customized replacement of Spark's built-in ParquetFileFormat. By so users don't have
to change existing Parquet-based SQL/code and will be able to read Arrow data from Parquet directly.
More importantly, sometimes the feature could be extremely helpful to make ArrowDataSource work correctly
with some 3rd-party storage tools (e.g. [Delta Lake](https://github.com/delta-io/delta)) that are built on top of ParquetDataSource.

To replace built-in ParquetDataSource, the only thing has to be done is to place compiled jar `spark-arrow-datasource-parquet-0.9.0.jar` into
Spark's library folder.

If you'd like to verify that ParquetDataSource is successfully overwritten by the jar, run following code 
before executing SQL job:
```
ServiceLoaderUtil.ensureParquetFileFormatOverwritten();
```

Note the whole feature is currently **experimental** and only DataSource v1 is supported. V2 support is being planned.