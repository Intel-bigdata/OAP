# OAP User Guide

* [Prerequisites](#Prerequisites)
* [Getting Started with OAP](#Getting-Started-with-OAP)
* [YARN Cluster and Spark Standalone Mode](#YARN-Cluster-and-Spark-Standalone-Mode)
* [Working with OAP Index](#Working-with-OAP-Index)
* [Working with OAP Cache](#Working-with-OAP-Cache)

## Prerequisites
Before getting started with OAP on Spark, you should have set up a working Hadoop cluster with YARN and Spark. Running Spark on YARN requires a binary distribution of Spark which is built with YARN support. If you don't want to build Spark by yourself, we have a pre-built Spark-2.3.2, you can download [Spark-2.3.2]() to your master machine.
## Getting Started with OAP
### Building OAP
We have a pre-built OAP, you can download [OAP-0.6.1.jar](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.0-spark-2.3.2/oap-0.6.0-with-spark-2.3.2.jar) to your master and put the OAP jar to directory such as `/home/oap/jars/`. If you’d like to build OAP from source code, please refer to [Developer Guide](./Developer-Guide.md).
### Spark Configurations for OAP
A common deploy mode that can be used to launch Spark applications on YRAN is ***client*** mode. The ***client*** mode is especially suitable for applications such as Spark shell. Before you run ` . $SPARK_HOME/bin/spark-shell ` to launch Spark, firstly you should add the following configurations in the file of `$SPARK_HOME/conf/spark-defaults.conf`

```
spark.master                      yarn
spark.deploy-mode                 client
spark.sql.extensions              org.apache.spark.sql.OapExtensions
spark.files                       /home/oap/jars/oap-0.6-with-spark-2.3.2.jar     # absolute path of OAP jar  
spark.executor.extraClassPath     ./oap-0.6-with-spark-2.3.2.jar                  # relative path of OAP jar
spark.driver.extraClassPath       /home/oap/jars/oap-0.6-with-spark-2.3.2.jar     # absolute path of OAP jar
```
### Verify Spark with OAP Integration 
After configuration, you can follow the below steps to run Spark shell and check if OAP configurations work. Here take data path `hdfs:///user/oap/` for example.

Step 1. Create data path on HDFS
```
hadoop fs -mkdir /user/oap/
```
Step 2. Run Spark shell
```
. $SPARK_HOME/bin/spark-shell
> spark.sql(s"""CREATE TABLE oap_test (a INT, b STRING)
       USING parquet
       OPTIONS (path 'hdfs:///user/oap/')""".stripMargin)
> val data = (1 to 30000).map { i => (i, s"this is test $i") }.toDF().createOrReplaceTempView("t")
> spark.sql("insert overwrite table oap_test select * from t")
> spark.sql("create oindex index1 on oap_test (a)")
> spark.sql("show oindex from oap_test").show()
```
When your Spark shell shows the same as below picture, it means you have run Spark with OAP successfully.

![Spark_shell_running_results](./image/spark_shell_oap.png)

## Configurations for YARN Cluster and Spark Standalone Mode
### YARN Cluster Mode
There are two deploy modes that can be used to launch Spark applications on YARN, ***client*** and ***cluster*** mode. if your application is submitted from a machine far from the worker machines (e.g. locally on your laptop), it is common to use ***cluster*** mode to minimize network latency between the drivers and the executors. Launching Applications with spark-submit can support different deploy modes that Spark supports, so you can run spark-submit to use YARN cluster mode.
#### Configurations on Spark with OAP on YARN Cluster Mode
Before run `spark-submit`, you should add below OAP configurations in the file of `$SPARK_HOME/conf/spark-defaults.conf`
```
spark.master                      yarn
spark.deploy-mode                 cluster
spark.sql.extensions              org.apache.spark.sql.OapExtensions
spark.files                       /home/oap/jars/oap-0.6-with-spark-2.3.2.jar        # absolute path    
spark.executor.extraClassPath     ./oap-0.6-with-spark-2.3.2.jar                     # relative path 
spark.driver.extraClassPath       ./oap-0.6-with-spark-2.3.2.jar                     # relative path
```
Then you can run `spark-submit` with YARN cluster mode
```
. $SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  ...
```
### Spark Standalone Mode
In addition to running on the YARN cluster managers, Spark also provides a simple standalone deploy mode. If install `Spark Standalone mode`, you simply place a compiled version of Spark and OAP on each node on the cluster.
```
spark.sql.extensions               org.apache.spark.sql.OapExtensions
spark.executor.extraClassPath      /home/oap/jars/oap-0.6-with-spark-2.3.2.jar      # absolute path
spark.driver.extraClassPath        /home/oap/jars/oap-0.6-with-spark-2.3.2.jar      # absolute path
```

## Working with OAP Index

You can use SQL DDL(create/drop/refresh/check/show index) to try OAP index function, run Spark with the following example to try OAP index function with Spark shell.
```
. $SPARK_HOME/bin/spark-shell
```
### Index Creation
Step 1. Use `CREATE` to create a table with `parquet` file format on corresponding HDFS data path `hdfs:///user/oap/`.
```
> spark.sql(s"""CREATE TABLE oap_test (a INT, b STRING)
       USING parquet
       OPTIONS (path 'hdfs:///user/oap/')""".stripMargin)
```
Step 2. Insert data into table `oap_test`.
```
> val data = (1 to 30000).map { i => (i, s"this is test $i") }.toDF().createOrReplaceTempView("t")
> spark.sql("insert overwrite table oap_test select * from t")
```
Step 3. Create index with OAP on `oap_test`
```
> spark.sql("create oindex index1 on oap_test (a)")
> spark.sql("show oindex from oap_test").show()
```
### Use OAP Index
```
> spark.sql("SELECT * FROM oap_test WHERE a = 1").show()
```
### Drop index
```
> spark.sql("drop oindex index1 on oap_test")
```

## Working with OAP Cache

If you want to run OAP with cache function, there are two media types in OAP to cache hot data: DRAM and DCPMM. 

### Use DRAM Cache 
Step 1. Change some configurations in `$SPARK_HOME/conf/spark-defaults.conf`. 

```
spark.memory.offHeap.enabled                true
spark.memory.offHeap.size                   80g      # half of total memory size
spark.sql.oap.parquet.data.cache.enable     true     #for parquet fileformat
spark.sql.oap.orc.data.cache.enable         true     #for orc fileformat
```
Step 2. Run Spark ***ThriftServer***

You should run Spark ***ThriftServer*** with the beeline scripts to use OAP DRAM cache, ThriftServer launchs Spark applications which can cache hot data for a long time backstage, and it can also accept query requests from different clients at the same time.

To directly verify DRAM Cache function, we reuse table `oap_test` created in the [Working with OAP Index](#Working-with-OAP-Index).

When we run ```spark-shell``` to create table `oap_test`, `metastore_db` will be created, so we need to run Thrift JDBC server in the same directory of `metastore`.
```
. $SPARK_HOME/sbin/start-thriftserver.sh
```
Step3. Use beeline and Connect to the JDBC/ODBC server in beeline with:

```
./beeline -u jdbc:hive2://vsr211:10000       
```
vsr211 is hostname, so you need change to your hostname.
 
Step 4. Using DRAM Cache on table `oap_test`
When ***0: jdbc:hive2://vsr211:10000>*** shows up, then you can directly use table oap_test, which is stored in the `default` database.
```
> SHOW databases;
> USE default;
> SHOW tables;
> USE oap_test;
```
Step 5. Run query like
```
> SELECT * FROM oap_test WHERE a = 1;
> SELECT * FROM oap_test WHERE a = 2;
> SELECT * FROM oap_test WHERE a = 3;
...
```
Then you can find the cache metric with OAP TAB in the spark history Web UI.

![webUI](./image/webUI.png)



### Use DCPMM Cache 
#### Prerequisites
When you want to use DCPMM to cache hot data, you should follow the below steps.

Step 1. You need have DCPMM formatted and mounted on your clusters.

Step 2. Download [libmemkind.so.0](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.0-spark-2.3.2/libmemkind.so.0), [libnuma.so.1](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.0-spark-2.3.2/libnuma.so.1) to directory `/lib64/`(Centos) in each executor node.
##### Achieve NUMA binding
Step 3. Install numactl by `yum install numactl -y ` to achieve NUMA binding
##### Configurations for DCPMM 
Step 4. Create a file named “persistent-memory.xml” under "$SPARK_HOME/conf/" and set the “initialPath” of numa node in “persistent-memory.xml”. You can directly copy the following part only changing `/mnt/pmem0` `/mnt/pmem1` to your path to DCPMM.

```
<persistentMemoryPool>
  <!--The numa id-->
  <numanode id="0">
    <!--The initial path for Intel Optane DC persistent memory-->
    <initialPath>/mnt/pmem0</initialPath>
  </numanode>
  <numanode id="1">
    <initialPath>/mnt/pmem1</initialPath>
  </numanode>
</persistentMemoryPool>
```

Here we privide you with an example, this cluster consists of 2 worker nodes, per node has 2 pieces of 488GB DCPMM ; 
```
spark.executor.instances                                   4               # 2x of number of your worker nodes
spark.yarn.numa.enabled                                    true            # enable numa
spark.executorEnv.MEMKIND_ARENA_NUM_PER_KIND               1
spark.memory.offHeap.enabled                               false
spark.speculation                                          false
spark.sql.oap.fiberCache.memory.manager                    pm              # use DCPMM as cache media
spark.sql.oap.fiberCache.persistent.memory.initial.size    450g            # ~90% of total available DCPMM per executor
spark.sql.oap.fiberCache.persistent.memory.reserved.size   30g             # the left DCPMM per executor
spark.sql.oap.parquet.data.cache.enable                    true            # for parquet fileformat
spark.sql.oap.orc.data.cache.enable                        true            # for orc fileformat
```
You can also run Spark with the same following example as DRAM cache to try OAP cache function with DCPMM, then you can find the cache metric with OAP TAB in the spark history Web UI.
