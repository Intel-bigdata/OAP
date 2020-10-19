# Spark Native SQL Engine Installation

For detailed testing scripts, please refer to [solution guide](https://github.com/Intel-bigdata/Solution_navigator/tree/master/nativesql)

## Install spark 3.0.0 or above

Download Spark from [download](https://spark.apache.org/downloads.html)

```
wget http://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz
tar -xf ./spark-3.0.0-bin-hadoop2.7.tgz
export SPARK_HOME=`pwd`/spark-3.0.0-bin-hadoop2.7
```

## Install arrow 0.17.0 &  Native-SQL 

We have provided a Conda package which will automatically install dependencies needed by OAP, you can refer to [OAP-Installation-Guide](../../docs/OAP-Installation-Guide.md) for more information. Once finished, you will get arrow 0.17.0 dependencies installed by Conda, and the compiled `spark-columnar-core` & `spark-arrow-datasource ` jar will be put into dir   `$HOME/miniconda2/envs/oapenv/oap_jars/`

When you finish [OAP-Installation-Guide](../../docs/OAP-Installation-Guide.md) , just jump to [Spark Configurations for Native SQL Engine](#spark-configurations-for-native-sql-engine).

### Manully Install arrow 0.17.0 
Step 1. Install arrow 0.17.0 dependencies
```
git clone https://github.com/intel-bigdata/arrow && cd arrow && git checkout branch-0.17.0-oap-0.9
vim ci/conda_env_gandiva.yml 
clangdev=7
llvmdev=7

conda create -y -n pyarrow-dev -c conda-forge \
    --file ci/conda_env_unix.yml \
    --file ci/conda_env_cpp.yml \
    --file ci/conda_env_python.yml \
    --file ci/conda_env_gandiva.yml \
    compilers \
    python=3.7

conda activate pyarrow-dev
```
Step2. Install arrow 0.17.0

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
cd OAP && git checkout branch-0.17.0-oap-0.9
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
spark.driver.extraClassPath $HOME/miniconda2/envs/oapenv/oap_jars/spark-columnar-core-0.9.0-jar-with-dependencies.jar:$HOME/miniconda2/envs/oapenv/oap_jars/spark-arrow-datasource-0.9.0-jar-with-dependencies.jar
spark.executor.extraClassPath $HOME/miniconda2/envs/oapenv/oap_jars/spark-columnar-core-0.9.0-jar-with-dependencies.jar:$HOME/miniconda2/envs/oapenv/oap_jars/spark-arrow-datasource-0.9.0-jar-with-dependencies.jar

######
```
About spark-arrow-datasource.jar, you can refer [Unified Arrow Data Source ](../../oap-data-source/arrow/README.md).                                                                                         
Here's one example to verify if native sql engine works, make sure you have TPC-H dataset.  We could do a simple projection on one parquet table. For detailed testing scripts, please refer to [solution guide](https://github.com/Intel-bigdata/Solution_navigator/tree/master/nativesql).
```
val orders = spark.read.format("arrow").load("hdfs:////user/root/date_tpch_10/orders")
orders.createOrReplaceTempView("orders")
spark.sql("select * from orders where o_orderdate > date '1998-07-26'").show(20000, false)
```

The result should showup on Spark console and there should be a similar diagram in the SQL page of Spark history:
![UI](/oap-native-sql/resource/historyui.png)

## Performance data

For initial microbenchmark performance, we add 10 fields up with spark, data size is 200G data

![Performance](/oap-native-sql/resource/performance.png)

## Coding Style

* For Java code, we used [google-java-format](https://github.com/google/google-java-format)
* For Scala code, we used [Spark Scala Format](https://github.com/apache/spark/blob/master/dev/.scalafmt.conf), please use [scalafmt](https://github.com/scalameta/scalafmt) or run ./scalafmt for scala codes format
* For Cpp codes, we used Clang-Format, check on this link [google-vim-codefmt](https://github.com/google/vim-codefmt) for details.
