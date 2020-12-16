# Spark Native SQL Engine

A Native Engine for Spark SQL with vectorized SIMD optimizations

## Introduction

![Overview](/oap-native-sql/resource/nativesql_arch.png)

Spark SQL works very well with structured row-based data. It used WholeStageCodeGen to improve the performance by Java JIT code. However Java JIT is usually not working very well on utilizing latest SIMD instructions, especially under complicated queries. [Apache Arrow](https://arrow.apache.org/) provided CPU-cache friendly columnar in-memory layout, its SIMD optimized kernels and LLVM based SQL engine Gandiva are also very efficient. Native SQL Engine used these technoligies and brought better performance to Spark SQL.

## Key Features

### Apache Arrow formatted intermediate data among Spark operator

![Overview](/oap-native-sql/resource/columnar.png)

With [Spark 27396](https://issues.apache.org/jira/browse/SPARK-27396) its possible to pass a RDD of Columnarbatch to operators. We implemented this API with Arrow columnar format.

### Apache Arrow based Native Readers for Parquet and other formats

![Overview](/oap-native-sql/resource/dataset.png)

A native parquet reader was developed to speed up the data loading. it's based on Apache Arrow Dataset. For details please check [Arrow Data Source](../oap-data-source/README.md)

### Apache Arrow Compute/Gandiva based operators

![Overview](/oap-native-sql/resource/kernel.png)

We implemented common operators based on Apache Arrow Compute and Gandiva. The SQL expression was compiled to one expression tree with protobuf and passed to native kernels. The native kernels will then evaluate the these expressions based on the input columnar batch.

### Native Columnar Shuffle Operator with efficient compression support

![Overview](/oap-native-sql/resource/shuffle.png)

We implemented columnar shuffle to improve the shuffle performance. With the columnar layout we could do very efficient data compression for different data format.

## Build the Plugin

### Building by Conda

If you already have a working Hadoop Spark Cluster, we provide a Conda package which will automatically install dependencies needed by OAP, you can refer to [OAP-Installation-Guide](../docs/OAP-Installation-Guide.md) for more information. Once finished [OAP-Installation-Guide](../docs/OAP-Installation-Guide.md), you can find built `spark-columnar-core-1.0.0-jar-with-dependencies.jar` under `$HOME/miniconda2/envs/oapenv/oap_jars`.
Then you can just skip below steps and jump to Getting Started [Get Started](#Get-started).

### Building by yourself

If you prefer to build from the source code on your hand, please follow below steps to set up your environment.

### Prerequisite
There are some requirements before you build the project.
Please check the document [Prerequisite](/oap-native-sql/resource/Prerequisite.md) and make sure you have already installed the software in your system.
If you are running a SPARK Cluster, please make sure all the software are installed in every single node.

### Installation
Please check the document [Installation Guide](/oap-native-sql/resource/Installation.md) 

### Configuration & Testing 
Please check the document [Configuration Guide](/oap-native-sql/resource/Configuration.md)

## Get started
To enable OAP NativeSQL Engine, the previous built jar `spark-columnar-core-1.0.0-jar-with-dependencies.jar` should be added to Spark configuration. We also recommend to use `spark-arrow-datasource-standard-1.0.0-jar-with-dependencies.jar`. We will demonstrate an example by using both jar files.
SPARK related options are:

* `spark.driver.extraClassPath` : Set to load jar file to driver.
* `spark.executor.extraClassPath` : Set to load jar file to executor.
* `jars` : Set to copy jar file to the executors when using yarn cluster mode.
* `spark.executorEnv.ARROW_LIBHDFS3_DIR` : Optional if you are using a custom libhdfs3.so.
* `spark.executorEnv.LD_LIBRARY_PATH` : Optional if you are using a custom libhdfs3.so.

For Spark Standalone Mode, please set the above value as relative path to the jar file.
For Spark Yarn Cluster Mode, please set the above value as absolute path to the jar file.

Example to run Spark Shell with ArrowDataSource jar file
```
${SPARK_HOME}/bin/spark-shell \
        --verbose \
        --master yarn \
        --driver-memory 10G \
        --conf spark.driver.extraClassPath=$PATH_TO_JAR/spark-arrow-datasource-standard-1.0.0-jar-with-dependencies.jar:$PATH_TO_JAR/spark-columnar-core-1.0.0-jar-with-dependencies.jar \
        --conf spark.executor.extraClassPath=$PATH_TO_JAR/spark-arrow-datasource-standard-1.0.0-jar-with-dependencies.jar:$PATH_TO_JAR/spark-columnar-core-1.0.0-jar-with-dependencies.jar \
        --conf spark.driver.cores=1 \
        --conf spark.executor.instances=12 \
        --conf spark.executor.cores=6 \
        --conf spark.executor.memory=20G \
        --conf spark.memory.offHeap.size=80G \
        --conf spark.task.cpus=1 \
        --conf spark.locality.wait=0s \
        --conf spark.sql.shuffle.partitions=72 \
        --conf spark.executorEnv.ARROW_LIBHDFS3_DIR="$PATH_TO_LIBHDFS3_DIR/" \
        --conf spark.executorEnv.LD_LIBRARY_PATH="$PATH_TO_LIBHDFS3_DEPENDENCIES_DIR"
        --jars $PATH_TO_JAR/spark-arrow-datasource-standard-1.0.0-jar-with-dependencies.jar,$PATH_TO_JAR/spark-columnar-core-1.0.0-jar-with-dependencies.jar
```

Here is one example to verify if native sql engine works, make sure you have TPC-H dataset.  We could do a simple projection on one parquet table. For detailed testing scripts, please refer to [Solution Guide](https://github.com/Intel-bigdata/Solution_navigator/tree/master/nativesql).
```
val orders = spark.read.format("arrow").load("hdfs:////user/root/date_tpch_10/orders")
orders.createOrReplaceTempView("orders")
spark.sql("select * from orders where o_orderdate > date '1998-07-26'").show(20000, false)
```

The result should showup on Spark console and you can check the DAG diagram with some Columnar Processing stage.


## Performance data

For initial microbenchmark performance, we add 10 fields up with spark, data size is 200G data

![Performance](/oap-native-sql/resource/performance.png)

## Coding Style

* For Java code, we used [google-java-format](https://github.com/google/google-java-format)
* For Scala code, we used [Spark Scala Format](https://github.com/apache/spark/blob/master/dev/.scalafmt.conf), please use [scalafmt](https://github.com/scalameta/scalafmt) or run ./scalafmt for scala codes format
* For Cpp codes, we used Clang-Format, check on this link [google-vim-codefmt](https://github.com/google/vim-codefmt) for details.

## Contact

chendi.xue@intel.com
binwei.yang@intel.com
