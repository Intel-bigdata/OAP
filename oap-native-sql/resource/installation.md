## Installation

For detailed testing scripts, please refer to [solution guide](https://github.com/Intel-bigdata/Solution_navigator/tree/master/nativesql)

### Installation option 1: For evaluation, simple and fast

#### install spark 3.0.0 or above

[spark download](https://spark.apache.org/downloads.html)

Remove original Arrow Jars inside Spark assemply folder
``` shell
yes | rm assembly/target/scala-2.12/jars/arrow-format-0.15.1.jar
yes | rm assembly/target/scala-2.12/jars/arrow-vector-0.15.1.jar
yes | rm assembly/target/scala-2.12/jars/arrow-memory-0.15.1.jar
```

#### install arrow 0.17.0 dependencies

```
git clone https://github.com/apache/arrow && cd arrow & git checkout arrow-0.17.0
vim ci/conda_env_gandiva.yml 
clangdev=7
llvmdev=7

conda create -y -n pyarrow-dev -c conda-forge \
    --file ci/conda_env_unix.yml \
    --file ci/conda_env_cpp.yml \
    --file ci/conda_env_python.yml \
    --file ci/conda_env_gandiva.yml \
    compilers \
    python=3.7 \
    pandas
conda activate pyarrow-dev
```

#### Build native-sql cpp

``` shell
git clone https://github.com/Intel-bigdata/OAP.git
cd OAP && git checkout branch-nativesql-spark-3.0.0
cd oap-native-sql
cp cpp/src/resources/libhdfs.so ${HADOOP_HOME}/lib/native/ 
cp cpp/src/resources/libprotobuf.so.13 /usr/lib64/
```

Download spark-columnar-core-1.0-jar-with-dependencies.jar to local, add classPath to spark.driver.extraClassPath and spark.executor.extraClassPath
``` shell
Internal Location: vsr602://mnt/nvme2/chendi/000000/spark-columnar-core-1.0-jar-with-dependencies.jar
```

Download spark-sql_2.12-3.1.0-SNAPSHOT.jar to ${SPARK_HOME}/assembly/target/scala-2.12/jars/spark-sql_2.12-3.1.0-SNAPSHOT.jar
``` shell
Internal Location: vsr602://mnt/nvme2/chendi/000000/spark-sql_2.12-3.1.0-SNAPSHOT.jar
```

### Installation option 2: For contribution, Patch and build

#### install spark 3.0.0 or above

Please refer this link to install Spark.
[Apache Spark Installation](/oap-native-sql/resource/SparkInstallation.md)

Remove original Arrow Jars inside Spark assemply folder
``` shell
yes | rm assembly/target/scala-2.12/jars/arrow-format-0.15.1.jar
yes | rm assembly/target/scala-2.12/jars/arrow-vector-0.15.1.jar
yes | rm assembly/target/scala-2.12/jars/arrow-memory-0.15.1.jar
```

#### install arrow 0.17.0

Please refer this markdown to install Apache Arrow and Gandiva.
[Apache Arrow Installation](/oap-native-sql/resource/ApacheArrowInstallation.md)

#### compile and install oap-native-sql

##### Install Googletest and Googlemock

``` shell
yum install gtest-devel
yum install gmock
```

##### Build this project

``` shell
git clone https://github.com/Intel-bigdata/OAP.git
cd OAP && git checkout branch-nativesql-spark-3.0.0
cd oap-native-sql
cd cpp/
mkdir build/
cd build/
cmake .. -DTESTS=ON
make -j
make install
#when deploying on multiple node, make sure all nodes copied libhdfs.so and libprotobuf.so.13
```

``` shell
cd SparkColumnarPlugin/core/
mvn clean package -DskipTests
```
### Additonal Notes
[Notes for Installation Issues](/oap-native-sql/resource/InstallationNotes.md)
  

## Spark Configuration

Add below configuration to spark-defaults.conf

```
##### Columnar Process Configuration

spark.sql.parquet.columnarReaderBatchSize 4096
spark.sql.sources.useV1SourceList avro
spark.sql.join.preferSortMergeJoin false
spark.sql.extensions com.intel.sparkColumnarPlugin.ColumnarPlugin
spark.shuffle.manager org.apache.spark.shuffle.sort.ColumnarShuffleManager

spark.driver.extraClassPath ${PATH_TO_OAP_NATIVE_SQL}/core/target/spark-columnar-core-0.9.0-jar-with-dependencies.jar
spark.executor.extraClassPath ${PATH_TO_OAP_NATIVE_SQL}/core/target/spark-columnar-core-0.9.0-jar-with-dependencies.jar

######
```
## Benchmark

For initial microbenchmark performance, we add 10 fields up with spark, data size is 200G data

![Performance](/oap-native-sql/resource/performance.png)

## Coding Style

* For Java code, we used [google-java-format](https://github.com/google/google-java-format)
* For Scala code, we used [Spark Scala Format](https://github.com/apache/spark/blob/master/dev/.scalafmt.conf), please use [scalafmt](https://github.com/scalameta/scalafmt) or run ./scalafmt for scala codes format
* For Cpp codes, we used Clang-Format, check on this link [google-vim-codefmt](https://github.com/google/vim-codefmt) for details.
