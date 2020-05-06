# OAP User Guide

* [Prerequisites](#Prerequisites)
* [Getting Started with OAP](#Getting-Started-with-OAP)
* [Configuration for YARN Cluster Mode](#Configuration-for-YARN-Cluster-Mode)
* [Configuration for Spark Standalone Mode](#Configuration-for-Spark-Standalone-Mode)
* [Working with OAP Index](#Working-with-OAP-Index)
* [Working with OAP Cache](#Working-with-OAP-Cache)
* [Run TPC-DS Benchmark for OAP Cache](#Run-TPC-DS-Benchmark-for-OAP-Cache)


## Prerequisites
Before getting started with OAP on Spark, you should have set up a working Hadoop cluster with YARN and Spark. Running Spark on YARN requires a binary distribution of Spark which is built with YARN support. If you don't want to build Spark by yourself, we have a pre-built Spark-2.4.4, you can download [Spark-2.4.4](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.1-spark-2.4.4/spark-2.4.4-bin-hadoop2.7-patched.tgz) and setup Spark on your working node.
## Getting Started with OAP
### Building OAP
We have a pre-built OAP, you can download [OAP-0.7.0 for Spark 2.4.4 jar](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.1-spark-2.4.4/oap-0.6.1-with-spark-2.4.4.jar) to your working node and put the OAP jar to your working directory such as `/home/oap/jars/`. If you’d like to build OAP from source code, please refer to [Developer Guide](Developer-Guide.md) for the detailed steps.
### Spark Configurations for OAP
Users usually test and run Spark SQL or Scala scripts in Spark Shell which launches Spark applications on YRAN with ***client*** mode. In this section, we will start with Spark Shell then introduce other use scenarios. 

Before you run ` . $SPARK_HOME/bin/spark-shell `, you need to configure Spark for OAP integration. You need to add or update the following configurations in the Spark configuration file `$SPARK_HOME/conf/spark-defaults.conf` on your working node.

```
spark.sql.extensions              org.apache.spark.sql.OapExtensions
spark.files                       /home/oap/jars/oap-0.7.0-with-spark-2.4.4.jar     # absolute path of OAP jar on your working node
spark.executor.extraClassPath     ./oap-0.7.0-with-spark-2.4.4.jar                  # relative path of OAP jar
spark.driver.extraClassPath       /home/oap/jars/oap-0.7.0-with-spark-2.4.4.jar     # absolute path of OAP jar on your working node
```
### Verify Spark with OAP Integration 
After configuration, you can follow the below steps and verify the OAP integration is working using Spark Shell.

Step 1. Create a test data path on your HDFS. Take data path `hdfs:///user/oap/` for example.
```
hadoop fs -mkdir /user/oap/
```
Step 2. Launch Spark Shell using the following command on your working node.
```
. $SPARK_HOME/bin/spark-shell
```
Steps 3. In Spark Shell, execute the following commands to test OAP integration. 
```
> spark.sql(s"""CREATE TABLE oap_test (a INT, b STRING)
       USING parquet
       OPTIONS (path 'hdfs:///user/oap/')""".stripMargin)
> val data = (1 to 30000).map { i => (i, s"this is test $i") }.toDF().createOrReplaceTempView("t")
> spark.sql("insert overwrite table oap_test select * from t")
> spark.sql("create oindex index1 on oap_test (a)")
> spark.sql("show oindex from oap_test").show()
```
The test creates an index on a table and then show the created index. If there is no error happens, it means the OAP jar is working with the configuration. The picture below is one example of a successfully run.

![Spark_shell_running_results](./image/spark_shell_oap.png)

## Configuration for YARN Cluster Mode
Spark Shell, Spark SQL CLI and Thrift Sever run Spark application in ***client*** mode. While Spark Submit tool can run Spark application in ***client*** or ***cluster*** mode deciding by --deploy-mode parameter. [Getting Started with OAP](#Getting-Started-with-OAP) session has shown the configurations needed for ***client*** mode. If you are running Spark Submit tool in ***cluster*** mode, you need to follow the below configuration steps instead.

Before run `spark-submit` with ***cluster*** mode, you should add below OAP configurations in the Spark configuration file `$SPARK_HOME/conf/spark-defaults.conf` on your working node.
```
spark.sql.extensions              org.apache.spark.sql.OapExtensions
spark.files                       /home/oap/jars/oap-0.7.0-with-spark-2.4.4.jar        # absolute path on your working node    
spark.executor.extraClassPath     ./oap-0.7.0-with-spark-2.4.4.jar                     # relative path 
spark.driver.extraClassPath       ./oap-0.7.0-with-spark-2.4.4.jar                     # relative path
```

## Configuration for Spark Standalone Mode
In addition to running on the YARN cluster manager, Spark also provides a simple standalone deploy mode. If you are using Spark in Spark Standalone mode, you need to copy the OAP jar to **all** the worker nodes. And then set the following configurations in “$SPARK_HOME/conf/spark-defaults” on working node.
```
spark.sql.extensions               org.apache.spark.sql.OapExtensions
spark.executor.extraClassPath      /home/oap/jars/oap-0.7.0-with-spark-2.4.4.jar      # absolute path on worker nodes
spark.driver.extraClassPath        /home/oap/jars/oap-0.7.0-with-spark-2.4.4.jar      # absolute path on worker nodes
```

## Working with OAP Index

After a successful OAP integration, you can use OAP SQL DDL to manage table indexes. The DDL operations include index create, drop, refresh and show. You can run Spark Shell to try and test these functions. The below index examples based on an example table created by the following commands in Spark Shell.

```
> spark.sql(s"""CREATE TABLE oap_test (a INT, b STRING)
       USING parquet
       OPTIONS (path 'hdfs:///user/oap/')""".stripMargin)
> val data = (1 to 30000).map { i => (i, s"this is test $i") }.toDF().createOrReplaceTempView("t")
> spark.sql("insert overwrite table oap_test select * from t")       
```

### Index Creation
Use CREATE OINDEX DDL command to create an B+ Tree index or bitmap index with given name on a table column. 
```
CREATE OINDEX index_name ON table_name (column_name) USING [BTREE, BITMAP]
```
The following example creates an B+ Tree index on column "a" of oap_test table.

```
> spark.sql("create oindex index1 on oap_test (a)")
```
###
Use SHOW OINDEX command to show all the created indexes on a specified table. For example,
```
> spark.sql("show oindex from oap_test").show()
```
### Use OAP Index
Using index in query is transparent. When the SQL queries have filter conditions on the column(s) which can take advantage to use the index to filter the data scan, the index will be automatically applied to the execution of Spark SQL. The following example will automatically use the underlayer index created on column "a".
```
> spark.sql("SELECT * FROM oap_test WHERE a = 1").show()
```
### Drop index
Use DROP OINDEX command to drop a named index.
```
> spark.sql("drop oindex index1 on oap_test")
```

## Working with OAP Cache

OAP is capable to provide input data cache functionality in executor. Considering utilizing the cache data among different SQL queries, we should configure to allow different SQL queries to use the same executor process. This can be achieved by running your queries through Spark ThriftServer. The below steps assume to use Spark ThriftServer. For cache media, we support both DRAM and Intel DCPMM which means you can choose to cache data in DRAM or Intel DCPMM if you have DCPMM configured in hardware.

### Use DRAM Cache 
Step 1. Make the following configuration changes in Spark configuration file `$SPARK_HOME/conf/spark-defaults.conf`. 

```
spark.memory.offHeap.enabled                   false
spark.sql.oap.fiberCache.memory.manager        offheap
spark.sql.oap.fiberCache.offheap.memory.size   50g      # equal to the size of executor.memoryOverhead
spark.executor.memoryOverhead                  50g      # according to the resource of cluster
spark.sql.oap.parquet.data.cache.enable        true     # for parquet fileformat
spark.sql.oap.orc.data.cache.enable            true     # for orc fileformat
spark.sql.orc.copyBatchToSpark                 true     # for orc fileformat
```
You should change the parameter `spark.sql.oap.fiberCache.offheap.memory.size` value according to the availability of DRAM capacity to cache data.

Step 2. Launch Spark ***ThriftServer***

After configuration, you can launch Spark Thift Server. And use Beeline command line tool to connect to the Thrift Server to execute DDL or DML operations. And the data cache will automatically take effect for Parquet or ORC file sources. To help you to do a quick verification of cache functionality, below steps will reuse database metastore created in the [Working with OAP Index](#Working-with-OAP-Index) which contains `oap_test` table definition. In production, Spark Thrift Server will have its own metastore database directory or metastore service and use DDL's  through Beeline for creating your tables.

When you run ```spark-shell``` to create table `oap_test`, `metastore_db` will be created in the directory from which you run '$SPARK_HOME/bin/spark-shell'. Go the same directory you ran Spark Shell and then execute the following command to launch Thrift JDBC server.
```
. $SPARK_HOME/sbin/start-thriftserver.sh
```
Step3. Use Beeline and connect to the Thrift JDBC server using the following command, replacing the hostname (mythriftserver) with your own Thrift Server hostname.

```
./beeline -u jdbc:hive2://mythriftserver:10000       
```
After the connection is established, execute the following command to check the metastore is initialized correctly.

```
> SHOW databases;
> USE default;
> SHOW tables;
```
 
Step 4. Run queries on table which will use the cache automatically. For example,

```
> SELECT * FROM oap_test WHERE a = 1;
> SELECT * FROM oap_test WHERE a = 2;
> SELECT * FROM oap_test WHERE a = 3;
...
```
Step 5. To verify that the cache functionality is in effect, you can open Spark History Web UI and go to OAP tab page. And check the cache metrics. The following picture is an example.

![webUI](./image/webUI.png)


### Use DCPMM Cache 
#### Prerequisites
Before configuring in OAP to use DCPMM cache, you need to make sure the following:

- DCPMM hardwares are installed, formatted and mounted correctly on every cluster worker node with AppDirect mode(refer [guide](https://software.intel.com/en-us/articles/quick-start-guide-configure-intel-optane-dc-persistent-memory-on-linux)). You will get a mounted directory to use if you have done this. Usually, the DCPMM on each socket will be mounted as a directory. For example, on a two sockets system, we may get two mounted directories named `/mnt/pmem0` and `/mnt/pmem1`.
```
	// use impctl command to show topology and dimm info of DCPM
	impctl show -topology
	impctl show -dimm
	// provision dcpm in app direct mode
	ipmctl create -goal PersistentMemoryType=AppDirect
	// reboot system to make configuration take affect
	reboot
	// check capacity provisioned for app direct mode(AppDirectCapacity)
	impctl show -memoryresources
	// show the DCPM region information
	impctl show -region
	// create namespace based on the region, multi namespaces can be created on a single region
	ndctl create-namespace -m fsdax -r region0
	ndctl create-namespace -m fsdax -r region1
	// show the created namespaces
	fdisk -l
	// create and mount file system
	mount -o dax /dev/pmem0 /mnt/pmem0
	mount -o dax /dev/pmem1 /mnt/pmem1
```
Above file systems are generated for 2 numa nodes, which can be checked by "numactl --hardware". For different number of numa nodes, corresponding number of namespaces should be created to assure correct file system paths mapping to numa nodes.
- [Memkind](http://memkind.github.io/memkind/) library has been installed on every cluster worker node if memkind/non-evictable cache strategies are chosen for DCPM cache. Please use the latest Memkind version. You can compile Memkind based on your system. We have a pre-build binary for x86 64bit CentOS Linux and you can download [libmemkind.so.0](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.1-spark-2.4.4/libmemkind.so.0) and put the file to `/lib64/` directory in each worker node in cluster. Memkind library depends on libnuma at the runtime. You need to make sure libnuma already exists in worker node system. To build memkind lib from source, you can:
```
     git clone https://github.com/memkind/memkind
     cd memkind
     ./autogen.sh
     ./configure
     make
     make install
```
- [Vmemcache](https://github.com/pmem/vmemcache) library has been installed on every cluster worker node if vmemcache strategy is chosen for DCPM cache. You can follow the build/install steps from vmemcache website and make sure libvmemcache.so exist in '/lib64' directory in each worker node. To build vmemcache lib from source, you can (for RPM-based linux as example):
```
     git clone https://github.com/pmem/vmemcache
     cd vmemcache
     mkdir build
     cd build
     cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DCPACK_GENERATOR=rpm
     make package
     sudo rpm -i libvmemcache*.rpm
```

#### Configure for NUMA
To achieve the optimum performance, we need to configure NUMA for binding executor to NUMA node and try access the right DCPMM device on the same NUMA node. You need install numactl on each worker node. For example, on CentOS, run following command to install numactl.

```yum install numactl -y ```

You also need to build spark from source to enable numa-binding support. Refer [enable-numa-binding-for-dcpmm-in-spark](./Developer-Guide.md#enable-numa-binding-for-dcpmm-in-spark).

#### Configure for DCPMM 
Create a configuration file named “persistent-memory.xml” under "$SPARK_HOME/conf/" if it doesn't exist. Use below contents as a template for 2 numa nodes server and change the “initialPath” to your mounted paths for DCPMM devices. 

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
#### Configure for Spark/OAP to enable DCPMM cache
Make the following configuration changes in Spark configuration file `$SPARK_HOME/conf/spark-defaults.conf`.

```
spark.executor.instances                                   6               # 2x number of your worker nodes
spark.yarn.numa.enabled                                    true            # enable numa
spark.executorEnv.MEMKIND_ARENA_NUM_PER_KIND               1
spark.memory.offHeap.enabled                               false
spark.speculation                                          false
spark.sql.oap.fiberCache.persistent.memory.initial.size    256g            # DCPMM capacity per executor
spark.sql.oap.fiberCache.persistent.memory.reserved.size   50g             # Reserved space per executor
spark.sql.extensions                  org.apache.spark.sql.OapExtensions   # Enable OAP jar in Spark
```
***Also need to add OAP jar absolute file path in spark.executor.extraClassPath and spark.driver.extraClassPath.***

You need to change the value for spark.executor.instances, spark.sql.oap.fiberCache.persistent.memory.initial.size, and spark.sql.oap.fiberCache.persistent.memory.reserved.size according to your real environment. 

- spark.executor.instances: We suggest to configure the value to 2x number of the worker nodes considering NUMA binding is enabled. With each worker node runs two executors, each executor will be bound to one of the two sockets. And accesses the corresponding DCPMM device on that socket.
- spark.sql.oap.fiberCache.persistent.memory.initial.size: It is configured to the available DCPMM capacity to used as data cache per exectutor.
- spark.sql.oap.fiberCache.persistent.memory.reserved.size: When we use DCPMM as memory through memkind library, some portion of the space needs to be reserved for memory management overhead, such as memory segmentation. We suggest reserving 20% - 25% of the available DCPMM capacity to avoid memory allocation failure. But even with an allocation failure, OAP will continue the operation to read data from original input data and will not cache the data block.

***Besides above configurations, there are other specific parameters based on different DCPM cache strategies(guava, non-evictable, vmemcache). You can choose one of them and follow corresponding guide as following.***

#### Use Guava Cache Strategy

Guava cache is based on memkind library, built on top of jemalloc and provides memory characteristics. To use it in your workload, follow [prerequisites](#prerequisites-1) to set up DCPMM hardware and memkind library correctly. Then follow bellow configurations.

Memkind library also support DAX KMEM mode. Refer [Kernel](https://github.com/memkind/memkind#kernel), this chapter will guide how to configure persistent memory as system ram. Or [Memkind support for KMEM DAX option](https://pmem.io/2020/01/20/memkind-dax-kmem.html) for more details.

Please note that DAX KMEM mode need kernel version 5.x and memkind version 1.10 or above.

For Parquet data format, provides following conf options:
```
spark.sql.oap.parquet.data.cache.enable           true
spark.sql.oap.fiberCache.memory.manager           pm / kmem
spark.oap.cache.strategy                          guava
spark.sql.oap.fiberCache.persistent.memory.initial.size    *g
spark.sql.extensions                              org.apache.spark.sql.OapExtensions
```
For Orc data format, provides following conf options:
```
spark.sql.orc.copyBatchToSpark                   true
spark.sql.oap.orc.data.cache.enable              true
spark.sql.oap.orc.enable                         true
spark.sql.oap.fiberCache.memory.manager          pm / kmem
spark.oap.cache.strategy                         guava
spark.sql.oap.fiberCache.persistent.memory.initial.size      *g
spark.sql.extensions                             org.apache.spark.sql.OapExtensions
```

#### Use Non-evictable Cache strategy

Another cache strategy named Non-evictable is also supported in OAP based on memkind library for DCPMM.

To apply Non-evictable cache strategy in your workload, please follow [prerequisites](#prerequisites-1) to set up DCPMM hardware and memkind library correctly. Then follow bellow configurations.

For Parquet data format, provides following conf options:
```
spark.sql.oap.parquet.data.cache.enable                  true 
spark.oap.cache.strategy                                 noevict 
spark.sql.oap.fiberCache.persistent.memory.initial.size  256g 
```
For Orc data format, provides following conf options:
```
spark.sql.orc.copyBatchToSpark                           true 
spark.sql.oap.orc.data.cache.enable                      true 
spark.oap.cache.strategy                                 noevict 
spark.sql.oap.fiberCache.persistent.memory.initial.size  256g 
```

#### Use Vmemcache Cache Strategy

Vmemcache cache strategy is implemented based on libvmemcache (buffer based LRU cache), which provides data store API using <key, value> as input. To use this strategy, follow [prerequisites](#prerequisites-1) to set up DCPMM hardware and vmemcache library correctly, then refer below configurations to apply vmemcache cache strategy in your workload.

For Parquet data format, provides following conf options:

```
 
spark.sql.oap.parquet.data.cache.enable                    true 
spark.oap.cache.strategy                                   vmem 
spark.sql.oap.fiberCache.persistent.memory.initial.size    256g 
spark.sql.oap.cache.guardian.memory.size                   10g      # according to your cluster
```

For Orc data format, provides following conf options:

```
spark.sql.orc.copyBatchToSpark                             true 
spark.sql.oap.orc.data.cache.enable                        true 
spark.oap.cache.strategy                                   vmem 
spark.sql.oap.fiberCache.persistent.memory.initial.size    256g
spark.sql.oap.cache.guardian.memory.size                   10g      # according to your cluster   

```
Note: If "PendingFiber Size" (on spark web-UI OAP page) is large, or some tasks failed due to "cache guardian use too much memory", user could set `spark.sql.oap.cache.guardian.memory.size ` to a larger number, and the default size is 10GB. Besides, user could increase `spark.sql.oap.cache.guardian.free.thread.nums` or decrease `spark.sql.oap.cache.dispose.timeout.ms` to accelerate memory free.

### Enabling Index/Data cache separation
OAP now supports different cache strategies, which includes `guava`, `vmemcache`, `simple` and `noevict`, for DRAM and DCPMM. To optimize the cache media utilization, you can enable cache separation of data and index with same or different cache media. When Sharing same media, data cache and index cache will use different fiber cache ratio.
Here we list 4 different kinds of configs for index/cache separation, if you choose one of them, please add corresponding configs to `spark-defaults.conf`.

1. DRAM(`offheap`) as cache media, strategy `guava` as index and data cache backend. 
```
spark.sql.oap.index.data.cache.separation.enable        true
spark.oap.cache.strategy                                mix
spark.sql.oap.fiberCache.memory.manager                 offheap
```
2. DCPMM(`pm`) as cache media, strategy `guava` as index and data cache backend. 
```
spark.sql.oap.index.data.cache.separation.enable        true
spark.oap.cache.strategy                                mix
spark.sql.oap.fiberCache.memory.manager                 pm
```
3. DRAM(`offheap`)/`guava` as `index` cache media and backend, DCPMM(`pm`)/`guava` as `data` cache media and backend. 
```
spark.sql.oap.index.data.cache.separation.enable        true
spark.oap.cache.strategy                                mix
spark.sql.oap.fiberCache.memory.manager                 mix 
spark.sql.oap.mix.index.memory.manager                  offheap
spark.sql.oap.mix.data.memory.manager                   pm
spark.sql.oap.mix.index.cache.backend                   guava
spark.sql.oap.mix.data.cache.backend                    guava
```
4. DRAM(`offheap`)/`guava` as `index` cache media and backend, DCPMM(`tmp`)/`vmem` as `data` cache media and backend. 
```
spark.sql.oap.index.data.cache.separation.enable        true
spark.oap.cache.strategy                                mix
spark.sql.oap.fiberCache.memory.manager                 mix 
spark.sql.oap.mix.index.memory.manager                  offheap
spark.sql.oap.mix.index.cache.backend                   guava
spark.sql.oap.mix.data.cache.backend                    vmem
```
### Enabling Binary cache 
We introduce binary cache for both Parquet and ORC file format to improve cache space utilization compared to ColumnVector cache. When enabling binary cache, you should add following configs to `spark-defaults.conf`.
```
spark.sql.oap.parquet.binary.cache.enabled                true      # for parquet fileformat
spark.sql.oap.parquet.data.cache.enable                   false     # for ColumnVector, default is false
spark.sql.oap.orc.binary.cache.enable                     true      # for orc fileformat
spark.sql.oap.orc.data.cache.enable                       false     # for ColumnVector, default is false
```
#### Use External cache strategy

OAP supports arrow-plasma as external cache now and will support more other types in the future.[Plasma](http://arrow.apache.org/blog/2017/08/08/plasma-in-memory-object-store/) is a high-performance shared-memory object store.

Provide the following conf options:

```
--conf spark.oap.cache.strategy=external
--conf spark.sql.oap.cache.external.client.pool.size=30
```
[Apache Arrow](https://github.com/apache/arrow) source code is modified to support DCPMM.Here's the modified [repo](https://github.com/Intel-bigdata/arrow).


#### Verify DCPMM cache functionality

After the configuration, and you need to restart Spark Thrift Server to make the configuration changes taking effect. You can take the same steps described in [Use DRAM Cache](#Use-DRAM-Cache) to test and verify the cache is in working. 

Besides, you can verify numa binding status by confirming keywords like "numactl --cpubind=1 --membind=1" contained in executor launch command.

You can also check DCPM cache size by checking the usage of disk space using command 'df -h'. For Guava/Non-evictable strategies, the command will show disk space usage increases along with workload execution. But for vmemcache strategy, you will see disk usage becomes to cache initial size once DCPM cache initialized even workload haven't actually used so much space and this value doesn't change during workload execution.

## Run TPC-DS Benchmark for OAP Cache

The section provides instructions and tools for running TPC-DS queries to evaluate the cache performance at various configurations. TPC-DS suite has many queries and we select 9 I/O intensive queries for making the performance evaluation simple.

We created some tool scripts [OAP-TPCDS-TOOL.zip](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.1-spark-2.4.4/OAP-TPCDS-TOOL.zip) to simplify the running work for beginners. If you have already been familiar with TPC-DS data generation and running a TPC-DS tool suite, you can skip our tool and use TPC-DS tool suite directly.

#### Prerequisites

- The tool use Python scripts to execute Beeline commands to Spark Thrift Server. You need to install python 2.7+ on your working node.

#### Prepare the Tool
2. Download the [OAP-TPCDS-TOOL.zip](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.1-spark-2.4.4/OAP-TPCDS-TOOL.zip)  and unzip to a folder (for example, OAP-TPCDS-TOOL folder) on your working node. 
3. Copy OAP-TPCDS-TOOL/tools/tpcds-kits to ALL worker nodes under the same folder (for example, /home/oap/tpcds-kits).

#### Generate TPC-DS Data

1. Update the values for the following variables in OAP-TPCDS-TOOL/scripts/tool.conf based on your environment and needs.
- SPARK_HOME: Point to the Spark home directory of your Spark setup.
- TPCDS_KITS_DIR: The tpcds-kits directory you coped to the worker nodes in the above prepare process. For example, /home/oap/tpcds-kits
- NAMENODE_ADDRESS: Your HDFS Namenode address in the format of host:port.
- THRIFT_SERVER_ADDRESS: Your working node address on which you will run Thrift Server.
- DATA_SCALE: The data scale to be generated in GB
- DATA_FORMAT: The data file format. You can specify parquet or orc

The following is an example:

```
export SPARK_HOME=/home/oap/spark-2.4.4
export TPCDS_KITS_DIR=/home/oap/tpcds-kits
export NAMENODE_ADDRESS=mynamenode:9000
export THRIFT_SERVER_ADDRESS=mythriftserver
export DATA_SCALE=2
export DATA_FORMAT=parquet
```
2. Start data generation
At the root directory of this tool, for example, OAP-TPCDS-TOOL folder, execute scripts/run_gen_data.sh to start the data generation process. 
```
cd OAP-TPCDS-TOOL
sh ./scripts/run_gen_data.sh
```
Once finished, the data with $scale will be generated at HDFS folder genData$scale. And database with the name "tpcds$scale" was created with the TPC-DS tables.

#### Start Spark Thrift Server

You need to start the Thrift Server in the tool root folder, which is the same folder you run data generation scripts. We provide two different scripts to start Thrift Server for DCPMM and DRAM respectively.

##### Use DCPMM as cache
If you are about to use DCPMM as cache, use scripts/spark_thrift_server_yarn_with_DCPMM.sh. You need to update the configuration values in this script to reflect the real environment. Normally, you need to update the following configuration values for DCPMM case,
- --driver-memory
- --executor-memory
- --executor-cores
- --conf spark.sql.oap.fiberCache.persistent.memory.initial.size
- --conf spark.sql.oap.fiberCache.persistent.memory.reserved.size

These configurations will override the values specified in Spark configuration file. After the configuration is done, you can execute the following command to start Thrift Server.

```
cd OAP-TPCDS-TOOL
sh ./scripts/spark_thrift_server_yarn_with_DCPMM.sh start
```

##### Use DRAM as cache
If you are about to use DRAM as cache, use scripts/spark_thrift_server_yarn_with_DRAM.sh. You need to update the configuration values in this script to reflect the real environment. Normally, you need to update the following configuration values for DRAM case,
- --driver-memory
- --executor-memory
- --executor-cores
- --conf spark.memory.offHeap.size

These configurations will override the values specified in Spark configuration file. After the configuration is done, you can execute the following command to start Thrift Server.
```
cd OAP-TPCDS-TOOL
sh ./scripts/spark_thrift_server_yarn_with_DRAM.sh  start
```
#### Run Queries
Now you are ready to execute the queries over the data. Execute the following command to start to run queries.

```
cd OAP-TPCDS-TOOL
sh ./scripts/run_tpcds.sh
```

When all the queries are done, you will see the result.json file in the current directory.
