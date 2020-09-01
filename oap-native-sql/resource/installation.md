# Spark Native SQL Engine Installation

For detailed testing scripts, please refer to [solution guide](https://github.com/Intel-bigdata/Solution_navigator/tree/master/nativesql)

## Install spark 3.0.0 or above

Download Spark from [download](https://spark.apache.org/downloads.html)

```
wget http://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz
tar -xf ./spark-3.0.0-bin-hadoop2.7.tgz
export SPARK_HOME=`pwd`/spark-3.0.0-bin-hadoop2.7
```

## Install by Conda 
We provided a Conda package which will automatically install most dependencies, you can refer to [Conda-Installation-Guide](../../../docs/Conda-Installation-Guide.md) for more information. If you have finished [Conda-Installation-Guide](../../../docs/Conda-Installation-Guide.md), you needn't  follow the build steps.


## Install arrow 0.17.0 dependencies

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

### Install arrow 0.17.0

Please refer this doc to install Apache Arrow and Gandiva.
[Apache Arrow Installation](/oap-native-sql/resource/ApacheArrowInstallation.md)

## compile and install oap-native-sql

### Install Googletest and Googlemock

``` shell
yum install gtest-devel
yum install gmock
```

### Build Native SQL Engine

``` shell
git clone https://github.com/Intel-bigdata/OAP.git
cd OAP && git checkout branch-nativesql-spark-3.0.0
cd oap-native-sql
cd cpp/
mkdir build/
cd build/
cmake .. -DTESTS=ON
make -j
#when deploying on multiple node, make sure all nodes copied libhdfs.so and libprotobuf.so.13
```

``` shell
cd ../../core/
mvn clean package -DskipTests
```

### Additonal Notes
[Notes for Installation Issues](/oap-native-sql/resource/InstallationNotes.md)
  

### Spark Configurations for Native SQL Engine

Add below configuration to spark-defaults.conf

```
##### Columnar Process Configuration

spark.sql.sources.useV1SourceList avro
spark.sql.join.preferSortMergeJoin false
spark.sql.extensions com.intel.oap.ColumnarPlugin
spark.shuffle.manager org.apache.spark.shuffle.sort.ColumnarShuffleManager

# note native sql engine depends on arrow data source
spark.driver.extraClassPath ${PATH_TO_OAP_NATIVE_SQL}/core/target/spark-columnar-core-0.9.0-jar-with-dependencies.jar:${PATH_TO_OAP_DATA_SOURCE}/arrow/target/spark-arrow-datasource-0.9.0-SNAPSHOT-jar-with-dependencies.jar
spark.executor.extraClassPath ${PATH_TO_OAP_NATIVE_SQL}/core/target/spark-columnar-core-0.9.0-jar-with-dependencies.jar:${PATH_TO_OAP_DATA_SOURCE}/arrow/target/spark-arrow-datasource-0.9.0-SNAPSHOT-jar-with-dependencies.jar

######
```
Verify if native sql engine works with scala script or jupyter notebook:
```
val orders = spark.read.format("arrow").load("hdfs:////user/root/date_tpch_10/orders")
orders.createOrReplaceTempView("orders")
spark.sql("select o_orderdate from orders").show
```

## Performance data

For initial microbenchmark performance, we add 10 fields up with spark, data size is 200G data

![Performance](/oap-native-sql/resource/performance.png)

## Coding Style

* For Java code, we used [google-java-format](https://github.com/google/google-java-format)
* For Scala code, we used [Spark Scala Format](https://github.com/apache/spark/blob/master/dev/.scalafmt.conf), please use [scalafmt](https://github.com/scalameta/scalafmt) or run ./scalafmt for scala codes format
* For Cpp codes, we used Clang-Format, check on this link [google-vim-codefmt](https://github.com/google/vim-codefmt) for details.
