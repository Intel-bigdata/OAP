# User Guide

* [Prerequisites](#Prerequisites)
* [Getting Started](#Getting-Started)
* [Configuration for YARN Cluster Mode](#Configuration-for-YARN-Cluster-Mode)
* [Configuration for Spark Standalone Mode](#Configuration-for-Spark-Standalone-Mode)
* [Working with SQL Index](#Working-with-SQL-Index)
* [Working with SQL Data Source Cache](#Working-with-SQL-Data-Source-Cache)
* [Run TPC-DS Benchmark](#Run-TPC-DS-Benchmark)


## Prerequisites

SQL Index and Data Source Cache on Spark requires a working Hadoop cluster with YARN and Spark. Running Spark on YARN requires a binary distribution of Spark, which is built with YARN support. If you don't want to build Spark by yourself, we have pre-built [Spark-2.4.4](https://github.com/Intel-bigdata/spark/releases/download/v2.4.4-intel-oap-0.8/spark-2.4.4-bin-hadoop2.7-intel-oap-0.8.tgz ).

## Getting Started

### Building

Download our pre-built jar [oap-0.8.1-bin-spark-2.4.4.tar.gz](https://github.com/Intel-bigdata/OAP/releases/download/v0.8.1-spark-2.4.4/oap-0.8.1-bin-spark-2.4.4.tar.gz) to your working node, unzip it and put the jars to your working directory such as `/home/oap/jars/`, and put the `oap-common-0.8.1-with-spark-2.4.4.jar` to the directory `$SPARK_HOME/jars/`. If you’d like to build from source code, please refer to [Developer Guide](Developer-Guide.md) for the detailed steps.

### Spark Configurations

Users usually test and run Spark SQL or Scala scripts in Spark Shell,  which launches Spark applications on YRAN with ***client*** mode. In this section, we will start with Spark Shell then introduce other use scenarios. 

Before you run ` . $SPARK_HOME/bin/spark-shell `, you need to configure Spark for integration. You need to add or update the following configurations in the Spark configuration file `$SPARK_HOME/conf/spark-defaults.conf` on your working node.

```
spark.sql.extensions              org.apache.spark.sql.OapExtensions
# absolute path of the jar on your working node
spark.files                       /home/oap/jars/oap-cache-0.8.1-with-spark-2.4.4.jar
# relative path of the jar
spark.executor.extraClassPath     ./oap-cache-0.8.1-with-spark-2.4.4.jar
# absolute path of the jar on your working node
spark.driver.extraClassPath       /home/oap/jars/oap-cache-0.8.1-with-spark-2.4.4.jar
```
### Verify Integration 

After configuration, you can follow these steps to verify the OAP integration is working using Spark Shell.

1. Create a test data path on your HDFS. `hdfs:///user/oap/` for example.
   ```
   hadoop fs -mkdir /user/oap/
   
   ```
2. Launch Spark Shell using the following command on your working node.
   ``` 
   . $SPARK_HOME/bin/spark-shell
   ```

3. Execute the following commands in Spark Shell to test OAP integration. 
   ```
   > spark.sql(s"""CREATE TABLE oap_test (a INT, b STRING)
          USING parquet
          OPTIONS (path 'hdfs:///user/oap/')""".stripMargin)
   > val data = (1 to 30000).map { i => (i, s"this is test $i") }.toDF().createOrReplaceTempView("t")
   > spark.sql("insert overwrite table oap_test select * from t")
   > spark.sql("create oindex index1 on oap_test (a)")
   > spark.sql("show oindex from oap_test").show()
   ```

This test creates an index for a table and then shows it. If there are no errors, the OAP `.jar` is working with the configuration. The picture below is an example of a successfully run.

![Spark_shell_running_results](./image/spark_shell_oap.png)

## Configuration for YARN Cluster Mode

Spark Shell, Spark SQL CLI and Thrift Sever run Spark application in ***client*** mode. While Spark Submit tool can run Spark application in ***client*** or ***cluster*** mode deciding by --deploy-mode parameter. [Getting Started](#Getting-Started) session has shown the configurations needed for ***client*** mode. If you are running Spark Submit tool in ***cluster*** mode, you need to follow the below configuration steps instead.

Add the following OAP configuration settings to `$SPARK_HOME/conf/spark-defaults.conf` on your working node before running `spark-submit` in ***cluster*** mode.
```
spark.sql.extensions              org.apache.spark.sql.OapExtensions
# absolute path on your working node
spark.files                       /home/oap/jars/oap-cache-0.8.1-with-spark-2.4.4.jar
# relative path    
spark.executor.extraClassPath     ./oap-cache-0.8.1-with-spark-2.4.4.jar
# relative path 
spark.driver.extraClassPath       ./oap-cache-0.8.1-with-spark-2.4.4.jar
```

## Configuration for Spark Standalone Mode

In addition to running on the YARN cluster manager, Spark also provides a simple standalone deploy mode. If you are using Spark in Spark Standalone mode:

1. Copy the OAP `.jar` to **all** the worker nodes. 
2. Add the following configuration settings to “$SPARK_HOME/conf/spark-defaults” on the working node.
```
spark.sql.extensions               org.apache.spark.sql.OapExtensions
# absolute path on worker nodes
spark.executor.extraClassPath      /home/oap/jars/oap-cache-0.8.1-with-spark-2.4.4.jar
# absolute path on worker nodes
spark.driver.extraClassPath        /home/oap/jars/oap-cache-0.8.1-with-spark-2.4.4.jar
```

## Working with SQL Index

After a successful OAP integration, you can use OAP SQL DDL to manage table indexes. The DDL operations include `index create`, `drop`, `refresh`, and `show`. Test these functions using the following examples in Spark Shell.

```
> spark.sql(s"""CREATE TABLE oap_test (a INT, b STRING)
       USING parquet
       OPTIONS (path 'hdfs:///user/oap/')""".stripMargin)
> val data = (1 to 30000).map { i => (i, s"this is test $i") }.toDF().createOrReplaceTempView("t")
> spark.sql("insert overwrite table oap_test select * from t")       
```

### Index Creation

Use the CREATE OINDEX DDL command to create a B+ Tree index or bitmap index. 
``` 
CREATE OINDEX index_name ON table_name (column_name) USING [BTREE, BITMAP]
```
The following example creates a B+ Tree index on column "a" of the `oap_test` table.
``` 
> spark.sql("create oindex index1 on oap_test (a)")
```
Use SHOW OINDEX command to show all the created indexes on a specified table.
```
> spark.sql("show oindex from oap_test").show()
```
### Use Index

Using index in a query is transparent. When SQL queries have filter conditions on the column(s) which can take advantage of the index to filter the data scan, the index will automatically be applied to the execution of Spark SQL. The following example will automatically use the underlayer index created on column "a".
```
> spark.sql("SELECT * FROM oap_test WHERE a = 1").show()
```

### Drop index

Use DROP OINDEX command to drop a named index.
```
> spark.sql("drop oindex index1 on oap_test")
```
## Working with Data Source Cache

Data Source Cache can provide input data cache functionality to the executor. When using the cache data among different SQL queries, configure cache to allow different SQL queries to use the same executor process. Do this by running your queries through the Spark ThriftServer as shown below. For cache media, we support both DRAM and Intel DCPMM which means you can choose to cache data in DRAM or Intel DCPMM if you have DCPMM configured in hardware.

### Use DRAM Cache 

1. Make the following configuration changes in Spark configuration file `$SPARK_HOME/conf/spark-defaults.conf`. 

   ```
   spark.memory.offHeap.enabled                   false
   spark.sql.oap.fiberCache.memory.manager        offheap
   # equal to the size of executor.memoryOverhead
   spark.sql.oap.fiberCache.offheap.memory.size   50g
   # according to the resource of cluster
   spark.executor.memoryOverhead                  50g
   # for parquet fileformat
   spark.sql.oap.parquet.data.cache.enable        true
   # for orc fileformat
   spark.sql.oap.orc.data.cache.enable            true
   # for orc fileformat
   spark.sql.orc.copyBatchToSpark                 true
   ```

   Change `spark.sql.oap.fiberCache.offheap.memory.size` based on the availability of DRAM capacity to cache data.

2. Launch Spark ***ThriftServer***

   Launch Spark Thrift Server, and use the Beeline command line tool to connect to the Thrift Server to execute DDL or DML operations. The data cache will automatically take effect for Parquet or ORC file sources. 
   
   The rest of this section will show you how to do a quick verification of cache functionality. It will reuse the database metastore created in the [Working with Data Source Cache Index](#Working-with-SQL-Index) section, which creates the `oap_test` table definition. In production, Spark Thrift Server will have its own metastore database directory or metastore service and use DDL's through Beeline for creating your tables.

   When you run ```spark-shell``` to create the `oap_test` table, `metastore_db` will be created in the directory where you ran '$SPARK_HOME/bin/spark-shell'. Go to that directory and execute the following command to launch Thrift JDBC server.

   ```
   . $SPARK_HOME/sbin/start-thriftserver.sh
   ```

3. Use Beeline and connect to the Thrift JDBC server, replacing the hostname (mythriftserver) with your own Thrift Server hostname.

   ```
   ./beeline -u jdbc:hive2://mythriftserver:10000       
   ```

   After the connection is established, execute the following commands to check the metastore is initialized correctly.

   ```
   > SHOW databases;
   > USE default;
   > SHOW tables;
   ```
 
4. Run queries on the table that will use the cache automatically. For example,

   ```
   > SELECT * FROM oap_test WHERE a = 1;
   > SELECT * FROM oap_test WHERE a = 2;
   > SELECT * FROM oap_test WHERE a = 3;
   ...
   ```

5. Open the Spark History Web UI and go to the OAP tab page to see verify the cache metrics. The following picture is an example.

   ![webUI](./image/webUI.png)


### Use DCPMM Cache 

#### Prerequisites

The following are required to configure OAP to use DCPMM cache.
- DCPMM hardware is successfully deployed on each node in cluster.
- Directories exposing DCPMM hardware on each socket. For example, on a two socket system the mounted DCPMM directories should appear as `/mnt/pmem0` and `/mnt/pmem1`. Correctly installed DCPMM must be formatted and mounted on every cluster worker node.

   ```
   // use ipmctl command to show topology and dimm info of DCPM
   ipmctl show -topology
   ipmctl show -dimm
   // provision dcpm in app direct mode
   ipmctl create -goal PersistentMemoryType=AppDirect
   // reboot system to make configuration take affect
   reboot
   // check capacity provisioned for app direct mode(AppDirectCapacity)
   ipmctl show -memoryresources
   // show the DCPM region information
   ipmctl show -region
   // create namespace based on the region, multi namespaces can be created on a single region
   ndctl create-namespace -m fsdax -r region0
   ndctl create-namespace -m fsdax -r region1
   // show the created namespaces
   fdisk -l
   // create and mount file system
   echo y | mkfs.ext4 /dev/pmem0
   echo y | mkfs.ext4 /dev/pmem1
   mount -o dax /dev/pmem0 /mnt/pmem0
   mount -o dax /dev/pmem1 /mnt/pmem1
   ```

   In this case file systems are generated for 2 numa nodes, which can be checked by "numactl --hardware". For a different number of numa nodes, a corresponding number of namespaces should be created to assure correct file system paths mapping to numa nodes.


- For cache solution guava/non-evict, make sure [Memkind](http://memkind.github.io/memkind/) library installed on every cluster worker node. Compile Memkind based on your system or directly place our pre-built binary of [libmemkind.so.0](https://github.com/Intel-bigdata/OAP/releases/download/v0.8.1-spark-2.4.4/libmemkind.so.0) for x86_64 bit CentOS Linux in the `/lib64/`directory of each worker node in cluster. Build and install step can refer to [build and install memkind](./Developer-Guide.md#build-and-install-memkind)

- For cache solution Vmemcahe/external cache, make sure [Vmemcache](https://github.com/pmem/vmemcache) library has been installed on every cluster worker node if vmemcache strategy is chosen for DCPM cache. You can follow the build/install steps from vmemcache website and make sure libvmemcache.so exist in '/lib64' directory in each worker node. You can download [vmemcache RPM package](https://github.com/Intel-bigdata/OAP/releases/download/v0.8.1-spark-2.4.4/libvmemcache-0.8..rpm), and install it by running `rpm -i libvmemcache*.rpm`. Build and install step can refer to [build and install vmemcache](./Developer-Guide.md#build-and-install-vmemcache)

- Data Source Cache use Plasma as a node-level external cache service, the benefit of using external cache is data could be shared across process boundaries.  [Plasma](http://arrow.apache.org/blog/2017/08/08/plasma-in-memory-object-store/) is a high-performance shared-memory object store, it's a component of [Apache Arrow](https://github.com/apache/arrow). We have modified Plasma to support DCPMM, and open source on [Intel-bigdata Arrow](https://github.com/Intel-bigdata/arrow/tree/oap-master) repo. Build and install step can refer to [build and install plasma](./Developer-Guide.md#build-and-install-plasma)
 
Or you can refer to [Developer-Guide](../../../docs/Developer-Guide.md), there is a shell script to help you install these dependencies automatically.

#### Configure for NUMA

1. Install `numactl` to bind the executor to the DCPMM device on the same NUMA node. 

   ```yum install numactl -y ```

2. Build Spark from source to enable numa-binding support. Refer to [enable-numa-binding-for-dcpmm-in-spark](./Developer-Guide.md#enable-numa-binding-for-dcpmm-in-spark).

#### Configure for DCPMM 

Create `persistent-memory.xml` in `$SPARK_HOME/conf/` if it doesn't exist. Use the following template and change the `initialPath` to your mounted paths for DCPMM devices. 

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

#### Configure to enable DCPMM cache

Make the following configuration changes in `$SPARK_HOME/conf/spark-defaults.conf`.

```
# 2x number of your worker nodes
spark.executor.instances                                   6
# enable numa
spark.yarn.numa.enabled                                    true
spark.executorEnv.MEMKIND_ARENA_NUM_PER_KIND               1
spark.memory.offHeap.enabled                               false
spark.speculation                                          false
# DCPM capacity per executor
spark.sql.oap.fiberCache.persistent.memory.initial.size    256g
# Reserved space per executor
spark.sql.oap.fiberCache.persistent.memory.reserved.size   50g
# Enable OAP jar in Spark
spark.sql.extensions                  org.apache.spark.sql.OapExtensions
```

***Add OAP absolute path to `.jar` file in `spark.executor.extraClassPath` and` spark.driver.extraClassPath`.***

Change the values of `spark.executor.instances`, `spark.sql.oap.fiberCache.persistent.memory.initial.size`, and `spark.sql.oap.fiberCache.persistent.memory.reserved.size` to match your environment. 

- `spark.executor.instances`: We suggest setting the value to 2X the number of worker nodes when NUMA binding is enabled. Each worker node runs two executors, each executor is bound to one of the two sockets, and accesses the corresponding DCPMM device on that socket.
- `spark.sql.oap.fiberCache.persistent.memory.initial.size`: It is configured to the available DCPMM capacity to be used as data cache per exectutor.
- `spark.sql.oap.fiberCache.persistent.memory.reserved.size`: When we use DCPMM as memory through memkind library, some portion of the space needs to be reserved for memory management overhead, such as memory segmentation. We suggest reserving 20% - 25% of the available DCPMM capacity to avoid memory allocation failure. But even with an allocation failure, OAP will continue the operation to read data from original input data and will not cache the data block.

#### Choose additional configuration options

Optimize your environment by choosing a DCPMM caching strategy (guava, non-evictable, vmemcache, external cache using plasma). Following table shows features of each cache.
| guava | non-evictable | vmemcache | external cache |
| :----- | :----- | :----- | :-----|
| Use memkind lib to operate on PMem and guava cache strategy when data eviction happens. | Use memkind lib to operate on PMem and doesn't allow data eviction. | Use vmemache lib to operate on PMem and LRU cache strategy when data eviction happens. | Use vmemache lib to operate on PMem and LRU cache strategy when data eviction happens. |
| Need numa patch in Spark for better performance. | Need numa patch in Spark for better performance. | Need numa patch in Spark for better performance. | Doesn't need numa patch. |
| Suggest using 2 executors one node to keep aligned with PMem paths and numa nodes number. | Suggest using 2 executors one node to keep aligned with PMem paths and numa nodes number. | Suggest using 2 executors one node to keep aligned with PMem paths and numa nodes number. | Node-level cache so there are no limitation for executor number. |
| Cache data cleaned once executors exited. | Cache data cleaned once executors exited. | Cache data cleaned once executors exited. | No data loss when executors exit thus is friendly to dynamic allocation. But currently it has performance overhead than other cache solutions. |

##### Guava cache

Guava cache is based on memkind library, built on top of jemalloc and provides memory characteristics. To use it in your workload, follow [prerequisites](#prerequisites-1) to set up DCPMM hardware and memkind library correctly. Then follow bellow configurations.

For Parquet data format, add these conf options:
```
spark.sql.oap.parquet.data.cache.enable           true
spark.sql.oap.fiberCache.memory.manager           pm 
spark.oap.cache.strategy                          guava
spark.sql.oap.fiberCache.persistent.memory.initial.size    *g
spark.sql.extensions                              org.apache.spark.sql.OapExtensions
```
For Orc data format, add these conf options:
```
spark.sql.orc.copyBatchToSpark                   true
spark.sql.oap.orc.data.cache.enable              true
spark.sql.oap.orc.enable                         true
spark.sql.oap.fiberCache.memory.manager          pm 
spark.oap.cache.strategy                         guava
spark.sql.oap.fiberCache.persistent.memory.initial.size      *g
spark.sql.extensions                             org.apache.spark.sql.OapExtensions
```

Memkind library also support DAX KMEM mode. Refer [Kernel](https://github.com/memkind/memkind#kernel), this chapter will guide how to configure persistent memory as system ram. Or [Memkind support for KMEM DAX option](https://pmem.io/2020/01/20/memkind-dax-kmem.html) for more details.

Please note that DAX KMEM mode need kernel version 5.x and memkind version 1.10 or above. If you choose KMEM mode, change memory manager from `pm` to `kmem` as below.
```
spark.sql.oap.fiberCache.memory.manager           kmem
```

#### Non-evictable cache

The non-evictable cache strategy is also supported in OAP based on the memkind library for DCPMM.

To apply Non-evictable cache strategy in your workload, please follow [prerequisites](#prerequisites-1) to set up DCPMM hardware and memkind library correctly. Then follow bellow configurations.

For Parquet data format, add these conf options:
```
spark.sql.oap.parquet.data.cache.enable                  true 
spark.oap.cache.strategy                                 noevict 
spark.sql.oap.fiberCache.persistent.memory.initial.size  256g 
```
For Orc data format, add these conf options:
```
spark.sql.orc.copyBatchToSpark                           true 
spark.sql.oap.orc.data.cache.enable                      true 
spark.oap.cache.strategy                                 noevict 
spark.sql.oap.fiberCache.persistent.memory.initial.size  256g 
```

#### Vmemcache cache

The vmemcache cache strategy is based on libvmemcache (buffer based LRU cache), which provides a key-value store API. Follow these steps to enable vmemcache support in Data Source Cache.
To use this strategy, follow [prerequisites](#prerequisites-1) to set up DCPMM hardware and vmemcache library correctly, then refer below configurations to apply vmemcache cache strategy in your workload.

For Parquet data format, add these conf options:

```
spark.sql.oap.parquet.data.cache.enable                    true 
spark.oap.cache.strategy                                   vmem 
spark.sql.oap.fiberCache.persistent.memory.initial.size    256g 
# according to your cluster
spark.sql.oap.cache.guardian.memory.size                   10g
```

For Orc data format, provides following conf options:

```
spark.sql.orc.copyBatchToSpark                             true 
spark.sql.oap.orc.data.cache.enable                        true 
spark.oap.cache.strategy                                   vmem 
spark.sql.oap.fiberCache.persistent.memory.initial.size    256g
# according to your cluster
spark.sql.oap.cache.guardian.memory.size                   10g
```
Note: If "PendingFiber Size" (on spark web-UI OAP page) is large, or some tasks fail with "cache guardian use too much memory" error, set `spark.sql.oap.cache.guardian.memory.size ` to a larger number as the default size is 10GB. The user could also increase `spark.sql.oap.cache.guardian.free.thread.nums` or decrease `spark.sql.oap.cache.dispose.timeout.ms` to free memory more quickly.

#### External cache using plasma

External cache strategy is implemented based on arrow/plasma library. To use this strategy, follow [prerequisites](#prerequisites-1) to set up DCPMM hardware. Then install arrow rpm package which include plasma library and executable file and copy arrow-plasma.jar to your ***SPARK_HOME/jars*** directory. Refer below configurations to apply external cache strategy and start plasma service on each node and start your workload.

For Parquet data format, add these conf options:

```
spark.sql.oap.parquet.data.cache.enable                    true 
spark.oap.cache.strategy                                   external
# according to your cluster
spark.sql.oap.cache.guardian.memory.size                   10g
spark.sql.oap.cache.external.client.pool.size              10
```

For Orc data format, add these conf options:

```
spark.sql.orc.copyBatchToSpark                             true 
spark.sql.oap.orc.data.cache.enable                        true 
spark.oap.cache.strategy                                   external
# according to your cluster 
spark.sql.oap.cache.guardian.memory.size                   10g
spark.sql.oap.cache.external.client.pool.size              10
```

- Start plasma service manually

plasma config parameters:  
 ```
 -m  how much Bytes share memory plasma will use
 -s  Unix Domain sockcet path
 -e  using external store
     vmemcache: using vmemcahe as external store
     propertyFilePath: It's recommended to use propertyFilePath to pass parameters.
     Or you can write these parameters directly in your starting command. Use "?" to seperate different numaNodes.
 ```

You can start plasma service on each node as following command, and then you can run your workload.

```
plasma-store-server -m 15000000000 -s /tmp/plasmaStore -e vmemcache://propertyFilePath:/tmp/persistent-memory.properties  
```
or 
``` 
plasma-store-server -m 15000000000 -s /tmp/plasmaStore -e vmemcache://totalNumaNodeNum:2,\
numaNodeId1:1,initialPath1:/mnt/pmem0,requiredSize1:15000000,readPoolSize1:12,writePoolSize1:12\
?numaNodeId2:2,initialPath2:/mnt/pmem1,requiredSize2:15000000,readPoolSize2:12,writePoolSize2:12
```

An example persistent-memory.properties:

```
  # Example
  totalNumaNodeNum = 2
    
  numaNodeId1 = 1
  initialPath1 = /mnt/pmem0
  requiredSize1 = 15000000
  readPoolSize1 = 12 
  writePoolSize1 = 12
    
  numaNodeId2 = 2
  initialPath2 = /mnt/pmem1
  requiredSize2 = 15000000
  readPoolSize2 = 12 
  writePoolSize2 = 12
```

```requiredSize readPoolSize writePoolSize``` is optional,will use default value if you don't pass these three parameters.
But please remember to pass ```totalNumaNodeNum``` and ```initialPath```.

*Please note that parameters in the command will cover parameters in persistent-memory.properties.*

 Remember to kill `plasma-store-server` process if you no longer need cache, and you should delete `/tmp/plasmaStore` which is a Unix domain socket.  
  
- Use yarn to start plamsa service  
We can use yarn(hadoop version >= 3.1) to start plasma service, you should provide a json file like following.
```
{
  "name": "plasma-store-service",
  "version": 1,
  "components" :
  [
   {
     "name": "plasma-store-service",
     "number_of_containers": 3,
     "launch_command": "plasma-store-server -m 15000000000 -s /tmp/plasmaStore -e vmemcache://propertyFilePath:/tmp/persistent-memory.properties ",
     "resource": {
       "cpus": 1,
       "memory": 512
     }
   }
  ]
}
```

Run command  ```yarn app -launch plasma-store-service /tmp/plasmaLaunch.json``` to start plasma server.  
Run ```yarn app -stop plasma-store-service``` to stop it.  
Run ```yarn app -destroy plasma-store-service```to destroy it.


### Index/Data cache separation

Data Source Cache now supports different cache strategies for DRAM and DCPMM. To optimize the cache media utilization, you can enable cache separation of data and index with same or different cache media. When Sharing same media, data cache and index cache will use different fiber cache ratio.

Here we list 4 different kinds of configs for index/cache separation, if you choose one of them, please add corresponding configs to `spark-defaults.conf`.
1. DRAM(`offheap`) as cache media, `guava` strategy as index, and data cache back end. 

```
spark.sql.oap.index.data.cache.separation.enable        true
spark.oap.cache.strategy                                mix
spark.sql.oap.fiberCache.memory.manager                 offheap
```
The rest configurations can refer to the configurations of  [Use DRAM Cache](#use-dram-cache) 

2. DCPMM(`pm`) as cache media, `guava` strategy as index, and data cache back end. 

```
spark.sql.oap.index.data.cache.separation.enable        true
spark.oap.cache.strategy                                mix
spark.sql.oap.fiberCache.memory.manager                 pm
```
The rest configurations can refer to the configurations of [DCPMM Cache](#use-dcpmm-cache) and  [Guava cache](#guava-cache)

3. DRAM(`offheap`)/`guava` as `index` cache media and backend, DCPMM(`pm`)/`guava` as `data` cache media and backend. 

```
spark.sql.oap.index.data.cache.separation.enable         true
spark.oap.cache.strategy                                 mix
spark.sql.oap.fiberCache.memory.manager                  mix 

# 2x number of your worker nodes
spark.executor.instances                                 6
# enable numa
spark.yarn.numa.enabled                                  true
spark.executorEnv.MEMKIND_ARENA_NUM_PER_KIND             1
spark.memory.offHeap.enabled                             false
# DCPMM capacity per executor
spark.sql.oap.fiberCache.persistent.memory.initial.size  256g
# Reserved space per executor
spark.sql.oap.fiberCache.persistent.memory.reserved.size 50g

# equal to the size of executor.memoryOverhead
spark.sql.oap.fiberCache.offheap.memory.size   50g
# according to the resource of cluster
spark.executor.memoryOverhead                  50g
# for ORC file format
spark.sql.orc.copyBatchToSpark                 true
spark.sql.oap.orc.data.cache.enable            true
# for Parquet file format
spark.sql.oap.parquet.data.cache.enable        true
```
4. DRAM(`offheap`)/`guava` as `index` cache media and backend, DCPMM(`tmp`)/`vmem` as `data` cache media and backend. 

```
spark.sql.oap.index.data.cache.separation.enable         true
spark.oap.cache.strategy                                 mix
spark.sql.oap.fiberCache.memory.manager                  mix 
spark.sql.oap.mix.data.cache.backend                     vmem

# 2x number of your worker nodes
spark.executor.instances                                 6
# enable numa
spark.yarn.numa.enabled                                  true
spark.memory.offHeap.enabled                             false
# DCPMM capacity per executor
spark.sql.oap.fiberCache.persistent.memory.initial.size  256g
# according to your cluster
spark.sql.oap.cache.guardian.memory.size                 10g

# equal to the size of executor.memoryOverhead
spark.sql.oap.fiberCache.offheap.memory.size   50g
# according to the resource of cluster
spark.executor.memoryOverhead                  50g

# for orc file format
spark.sql.orc.copyBatchToSpark                 true
spark.sql.oap.orc.data.cache.enable            true
# for Parquet file format
spark.sql.oap.parquet.data.cache.enable        true
```

### Binary cache 

A binary cache is available for both Parquet and ORC file format to improve cache space utilization compared to ColumnVector cache. When enabling binary cache, you should change following configs in `spark-defaults.conf`.
```
# for parquet file format, enable binary cache
spark.sql.oap.parquet.binary.cache.enabled      true
# for parquet file format, disable ColumnVector cache
spark.sql.oap.parquet.data.cache.enable         false
# for ORC file format, enable binary cache
spark.sql.oap.orc.binary.cache.enable           true
# for ORC file format, disable ColumnVector cache
spark.sql.oap.orc.data.cache.enable             false
```
The rest configurations can follow above part according to different cache media and strategies.

#### Verify DCPMM cache functionality

After finishing configuration, restart Spark Thrift Server for the configuration changes to take effect. Start at step 2 of the [Use DRAM Cache](#use-dram-cache) guide to verify that cache is working correctly.

Verify NUMA binding status by confirming keywords like `numactl --cpubind=1 --membind=1` contained in executor launch command.

Check DCPMM cache size by checking disk space with `df -h`. For Guava/Non-evictable strategies, the command will show disk space usage increases along with workload execution. For vmemcache strategy, disk usage will reach the initial cache size once the DCPMM cache is initialized and will not change during workload execution.

## Run TPC-DS Benchmark

This section provides instructions and tools for running TPC-DS queries to evaluate the cache performance of various configurations. The TPC-DS suite has many queries and we select 9 I/O intensive queries to simplify performance evaluation.

We created some tool scripts [OAP-TPCDS-TOOL.zip](https://github.com/Intel-bigdata/OAP/releases/download/v0.8.1-spark-2.4.4/OAP-TPCDS-TOOL.zip) to simplify running the workload. If you are already familiar with TPC-DS data generation and running a TPC-DS tool suite, skip our tool and use the TPC-DS tool suite directly.

### Prerequisites

- Python 2.7+ is required on the working node. 

### Prepare the Tool

1. Download [OAP-TPCDS-TOOL.zip](https://github.com/Intel-bigdata/OAP/releases/download/v0.8.1-spark-2.4.4/OAP-TPCDS-TOOL.zip) and unzip to a folder (for example, `OAP-TPCDS-TOOL` folder) on your working node. 
2. Copy `OAP-TPCDS-TOOL/tools/tpcds-kits` to ALL worker nodes under the same folder (for example, `/home/oap/tpcds-kits`).

### Generate TPC-DS Data

1. Update the values for the following variables in `OAP-TPCDS-TOOL/scripts/tool.conf` based on your environment and needs.

   - SPARK_HOME: Point to the Spark home directory of your Spark setup.
   - TPCDS_KITS_DIR: The tpcds-kits directory you coped to the worker nodes in the above prepare process. For example, /home/oap/tpcds-kits
   - NAMENODE_ADDRESS: Your HDFS Namenode address in the format of host:port.
   - THRIFT_SERVER_ADDRESS: Your working node address on which you will run Thrift Server.
   - DATA_SCALE: The data scale to be generated in GB
   - DATA_FORMAT: The data file format. You can specify parquet or orc

   For example:

```
export SPARK_HOME=/home/oap/spark-2.4.4
export TPCDS_KITS_DIR=/home/oap/tpcds-kits
export NAMENODE_ADDRESS=mynamenode:9000
export THRIFT_SERVER_ADDRESS=mythriftserver
export DATA_SCALE=2
export DATA_FORMAT=parquet
```

2. Start data generation.

   In the root directory of this tool (`OAP-TPCDS-TOOL`), run `scripts/run_gen_data.sh` to start the data generation process. 

```
cd OAP-TPCDS-TOOL
sh ./scripts/run_gen_data.sh
```

   Once finished, the `$scale` data will be generated in the HDFS folder `genData$scale`. And a database called `tpcds$scale` will contain the TPC-DS tables.

### Start Spark Thrift Server

Start the Thrift Server in the tool root folder, which is the same folder you run data generation scripts. Use either the DCPMM or DRAM scrip to start the Thrift Server.

#### Use DCPMM as Cache Media

Update the configuration values in `scripts/spark_thrift_server_yarn_with_DCPMM.sh` to reflect your environment. 
Normally, you need to update the following configuration values to cache to DCPMM.

- --driver-memory
- --executor-memory
- --executor-cores
- --conf spark.sql.oap.fiberCache.persistent.memory.initial.size
- --conf spark.sql.oap.fiberCache.persistent.memory.reserved.size

These settings will override the values specified in Spark configuration file ( `spark-defaults.conf`). After the configuration is done, you can execute the following command to start Thrift Server.

```
cd OAP-TPCDS-TOOL
sh ./scripts/spark_thrift_server_yarn_with_DCPMM.sh start
```
In this script, we use `guava` as cache strategy for ColumerVecor cache. you can alter to Binary cache. Or you can use `vmem` as cache strategy for ColumnVector or Binary cache, then follow above corresponding instructions to config rightly.

#### Use DRAM as Cache Media 

Update the configuration values in `scripts/spark_thrift_server_yarn_with_DRAM.sh` to reflect your environment. Normally, you need to update the following configuration values to cache to DRAM.

- --driver-memory
- --executor-memory
- --executor-cores
- --conf spark.sql.oap.fiberCache.offheap.memory.size
- --conf spark.executor.memoryOverhead

These settings will override the values specified in Spark configuration file (`spark-defaults.conf`). After the configuration is done, you can execute the following command to start Thrift Server.

```
cd OAP-TPCDS-TOOL
sh ./scripts/spark_thrift_server_yarn_with_DRAM.sh  start
```

### Run Queries

Execute the following command to start to run queries.

```
cd OAP-TPCDS-TOOL
sh ./scripts/run_tpcds.sh
```

When all the queries are done, you will see the `result.json` file in the current directory.
