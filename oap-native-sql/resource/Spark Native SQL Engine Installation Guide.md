# Spark Native SQL Engine Installation

## Install Apache Spark

[Download](https://spark.apache.org/downloads.html) the latest Apache Spark with Hadoop 2.7 or 3.2+.

```bash
wget http://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz

sudo mkdir -p /opt/spark && sudo mv spark-3.0.1-bin-hadoop3.2.tgz /opt/spark

sudo cd /opt/spark && sudo tar -xf spark-3.0.1-bin-hadoop3.2.tgz

export SPARK_HOME=/opt/spark/spark-3.0.1-bin-hadoop3.2/
```

## Install Apache Arrow

### Install dependencies

[Unix dependencies](https://github.com/apache/arrow/blob/master/ci/conda_env_unix.yml)

[C++ dependencies](https://github.com/apache/arrow/blob/master/ci/conda_env_cpp.yml)

[Python dependencies](https://github.com/apache/arrow/blob/master/ci/conda_env_python.yml)

[Arrow Gandiva dependencies](https://github.com/apache/arrow/blob/master/ci/conda_env_gandiva.yml)

For convenience, `conda`  can be used to install them.

```bash
git clone https://github.com/apache/arrow && cd arrow

conda create --name -y pyarrow python=3
conda activate pyarrow

conda install -c conda-forge --file ci/conda_env_unix.yml --file ci/conda_env_cpp.yml --file ci/conda_env_python.yml --file ci/conda_env_gandiva.yml compilers pandas
```

### Build Apache Arrow from [Intel-bigdata/arrow](https://github.com/Intel-bigdata/arrow.git).

Supports LLVM 7 or 8. 

* Spark Native SQL Engine needs highly-optimized local-built LLVM and Clang to achieve best performance.

Git clone
```bash
git clone https://github.com/Intel-bigdata/arrow.git
cd arrow && git checkout oap-master
```

Build Arrow C++ libraries
```bash
cd ${ARROW_ROOT}
mkdir -p cpp/build && cd cpp/build

cmake -DARROW_GANDIVA_JAVA=ON -DARROW_GANDIVA=ON -DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_BOOST_USE_SHARED=ON -DARROW_JNI=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_LZ4=ON -DARROW_FILESYSTEM=ON -DARROW_JSON=ON -DARROW_BUILD_BENCHMARKS=ON ..

make -j
sudo make install
```

Build Arrow Java libraries
```bash
cd ${ARROW_ROOT}/java

mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=../cpp/build/release/ -DskipTests 
```

---


## Build Spark Native SQL Engine

### Install Google Test and Google Mock

For CentOS:
``` shell
yum install gtest-devel
yum install gmock
```

### Build Native SQL Engine

Build C++ part
``` shell
git clone https://github.com/Intel-bigdata/OAP.git && cd OAP/oap-native-sql/cpp
mkdir build && cd build

cmake .. -DTESTS=ON
make -j
```

Test C++ part
```bash
cd OAP/oap-native-sql/cpp/build/src
ctest
```


### Spark Configurations for Native SQL Engine

Append following configuration to `spark-defaults.conf`:

```bash
##### Columnar Process Configuration

spark.sql.sources.useV1SourceList avro
spark.sql.join.preferSortMergeJoin false
spark.sql.extensions com.intel.oap.ColumnarPlugin
spark.shuffle.manager org.apache.spark.shuffle.sort.ColumnarShuffleManager

# Native SQL Engine depends on Arrow data source
spark.driver.extraClassPath ${OAP_NATIVE_SQL_DIR}/core/target/spark-columnar-core-0.9.0-jar-with-dependencies.jar:${OAP_DATA_SOURCE}/arrow/target/spark-arrow-datasource-0.9.0-SNAPSHOT-jar-with-dependencies.jar
spark.executor.extraClassPath ${OAP_NATIVE_SQL_DIR}/core/target/spark-columnar-core-0.9.0-jar-with-dependencies.jar:${OAP_DATA_SOURCE}/arrow/target/spark-arrow-datasource-0.9.0-SNAPSHOT-jar-with-dependencies.jar
```
Verify if native SQL engine works with `Scala script` or `Jupyter Notebook`:
```scala
val orders = spark.read.format("arrow").load("hdfs:////user/root/date_tpch_10/orders")
orders.createOrReplaceTempView("orders")
spark.sql("select o_orderdate from orders").show
```

## Performance

For initial microbenchmark performance, we add 10 fields up with spark, data size is 200G data.

![Performance](/oap-native-sql/resource/performance.png)

## Coding Style

* **Java**: [google-java-format](https://github.com/google/google-java-format)
* **Scala**: [Spark Scala Format](https://github.com/apache/spark/blob/master/dev/.scalafmt.conf) (tool: [scalafmt](https://github.com/scalameta/scalafmt))
* **C++**: [Clang-Format](https://github.com/google/vim-codefmt)

## Test Scripts

[Test scripts](https://github.com/Intel-bigdata/Solution_navigator/tree/master/nativesql)

## Installation Troubleshooting

[Installation Troubleshooting Guide](/oap-native-sql/resource/Troubleshooting%20Guide%20for%20oap-native-sql%20Installation.md)
