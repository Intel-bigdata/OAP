# Advanced Configuration

In addition to usage information provided in [User Guide](User-Guide.md), we provide more strategies for SQL Index and Data Source Cache in this section.

Their needed dependencies like ***Memkind*** ,***Vmemcache*** and ***Plasma*** can be automatically installed when following [OAP Installation Guide](../../../docs/OAP-Installation-Guide.md), corresponding feature jars can be found under `$HOME/miniconda2/envs/oapenv/oap_jars`.

- [Additional Cache Strategies](#Additional-Cache-Strategies)  In addition to **external** cache strategy, SQL Data Source Cache also supports 3 other cache strategies: **guava**, **noevict**  and **vmemcache**.
- [Index and Data Cache Separation](#Index-and-Data-Cache-Separation)  To optimize the cache media utilization, SQL Data Source Cache supports cache separation of data and index, by using same or different cache media with DRAM and PMem.
- [Cache Hot Tables](#Cache-Hot-Tables)  Data Source Cache also supports caching specific tables according to actual situations, these tables are usually hot tables.
- [Column Vector Cache](#Column-Vector-Cache)  This document above uses **binary** cache as example for Parquet file format, if your cluster memory resources is abundant enough, you can choose ColumnVector data cache instead of binary cache for Parquet to spare computation time.
- [Large Scale and Heterogeneous Cluster Support](#Large-Scale-and-Heterogeneous-Cluster-Support) Introduce an external database to store cache locality info to support large-scale and heterogeneous clusters.

## Additional Cache Strategies

Following table shows features of 4 cache strategies on PMem.

| guava | noevict | vmemcache | external cache |
| :----- | :----- | :----- | :-----|
| Use memkind lib to operate on PMem and guava cache strategy when data eviction happens. | Use memkind lib to operate on PMem and doesn't allow data eviction. | Use vmemcache lib to operate on PMem and LRU cache strategy when data eviction happens. | Use Plasma/dlmalloc to operate on PMem and LRU cache strategy when data eviction happens. |
| Need numa patch in Spark for better performance. | Need numa patch in Spark for better performance. | Need numa patch in Spark for better performance. | Doesn't need numa patch. |
| Suggest using 2 executors one node to keep aligned with PMem paths and numa nodes number. | Suggest using 2 executors one node to keep aligned with PMem paths and numa nodes number. | Suggest using 2 executors one node to keep aligned with PMem paths and numa nodes number. | Node-level cache so there are no limitation for executor number. |
| Cache data cleaned once executors exited. | Cache data cleaned once executors exited. | Cache data cleaned once executors exited. | No data loss when executors exit thus is friendly to dynamic allocation. But currently it has performance overhead than other cache solutions. |

- For cache solution `guava/noevict`, make sure [Memkind](https://github.com/memkind/memkind/tree/v1.10.1-rc2) library installed on every cluster worker node. If you have finished [OAP Installation Guide](../../../docs/OAP-Installation-Guide.md), libmemkind will be installed. Or manually build and install it following [memkind-installation](./Developer-Guide.md#memkind-installation), then place `libmemkind.so.0` under `/lib64/` on each worker node.

- For cache solution `vmemcahe/external` cache, make sure [Vmemcache](https://github.com/pmem/vmemcache) library has been installed on every cluster worker node. If you have finished [OAP Installation Guide](../../../docs/OAP-Installation-Guide.md), libvmemcache will be installed. Or you can follow the [vmemcache-installation](./Developer-Guide.md#vmemcache-installation) steps and make sure `libvmemcache.so.0` exist under `/lib64/` directory on each worker node.

If you have followed [OAP Installation Guide](../../../docs/OAP-Installation-Guide.md), ***Memkind*** ,***Vmemcache*** and ***Plasma*** will be automatically installed. 
Or you can refer to [Developer-Guide](../../../docs/Developer-Guide.md), there is a shell script to help you install these dependencies automatically.

### Use PMem Cache 

#### Prerequisites

The following are required to configure OAP to use PMem cache.

- PMem hardware is successfully deployed on each node in cluster.

- Directories exposing PMem hardware on each socket. For example, on a two socket system the mounted PMem directories should appear as `/mnt/pmem0` and `/mnt/pmem1`. Correctly installed PMem must be formatted and mounted on every cluster worker node. You can follow these commands to destroy interleaved PMem device which you set in [User-Guide](./User-Guide.md#prerequisites-1):

```
  # destroy interleaved PMem device which you set when using external cache strategy
  umount /mnt/pmem
  dmsetup remove striped-pmem
  echo y | mkfs.ext4 /dev/pmem0
  echo y | mkfs.ext4 /dev/pmem1
  mkdir -p /mnt/pmem0
  mkdir -p /mnt/pmem1
  mount -o dax /dev/pmem0 /mnt/pmem0
  mount -o dax /dev/pmem1 /mnt/pmem1
```

   In this case file systems are generated for 2 NUMA nodes, which can be checked by "numactl --hardware". For a different number of NUMA nodes, a corresponding number of namespaces should be created to assure correct file system paths mapping to NUMA nodes.
   
   For more information you can refer to [Quick Start Guide: Provision Intel® Optane™ DC Persistent Memory](https://software.intel.com/content/www/us/en/develop/articles/quick-start-guide-configure-intel-optane-dc-persistent-memory-on-linux.html)

#### Configuration for NUMA

1. Install `numactl` to bind the executor to the PMem device on the same NUMA node. 

   ```yum install numactl -y ```

2. We strongly recommend you use NUMA-patched Spark to achieve better performance gain for the following 3 cache strategies. Besides, currently using Community Spark occasionally has the problem of two executors being bound to the same PMem path. 
   
   Build Spark from source to enable NUMA-binding support, refer to [Enabling-NUMA-binding-for-PMem-in-Spark](./Developer-Guide.md#Enabling-NUMA-binding-for-PMem-in-Spark). 

  
#### Configuration for PMem 

Create `persistent-memory.xml` under `$SPARK_HOME/conf` if it doesn't exist. Use the following template and change the `initialPath` to your mounted paths for PMem devices. 

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

### Guava cache

Guava cache is based on memkind library, built on top of jemalloc and provides memory characteristics. To use it in your workload, follow [prerequisites](#prerequisites) to set up PMem hardware correctly, also make sure memkind library installed. Then follow configurations below.

**NOTE**: `spark.executor.sql.oap.cache.persistent.memory.reserved.size`: When we use PMem as memory through memkind library, some portion of the space needs to be reserved for memory management overhead, such as memory segmentation. We suggest reserving 20% - 25% of the available PMem capacity to avoid memory allocation failure. But even with an allocation failure, OAP will continue the operation to read data from original input data and will not cache the data block.

```
# enable numa
spark.yarn.numa.enabled                                        true
spark.executorEnv.MEMKIND_ARENA_NUM_PER_KIND                   1
# for Parquet file format, enable binary cache
spark.sql.oap.parquet.binary.cache.enabled                     true
# for ORC file format, enable binary cache
spark.sql.oap.orc.binary.cache.enabled                         true

spark.sql.oap.cache.memory.manager                             pm 
spark.oap.cache.strategy                                       guava
# PMem capacity per executor, according to your cluster
spark.executor.sql.oap.cache.persistent.memory.initial.size    256g
# Reserved space per executor, according to your cluster
spark.executor.sql.oap.cache.persistent.memory.reserved.size   50g
# enable SQL Index and Data Source Cache jar in Spark
spark.sql.extensions                                           org.apache.spark.sql.OapExtensions
# absolute path of the jar on your working node, when in Yarn client mode
spark.files                       $HOME/miniconda2/envs/oapenv/oap_jars/oap-cache-<version>-with-spark-<version>.jar,$HOME/miniconda2/envs/oapenv/oap_jars/oap-common-<version>-with-spark-<version>.jar
# relative path of the jar, when in Yarn client mode
spark.executor.extraClassPath     ./oap-cache-<version>-with-spark-<version>.jar:./oap-common-<version>-with-spark-<version>.jar
# absolute path of the jar on your working node,when in Yarn client mode
spark.driver.extraClassPath       $HOME/miniconda2/envs/oapenv/oap_jars/oap-cache-<version>-with-spark-<version>.jar:$HOME/miniconda2/envs/oapenv/oap_jars/oap-common-<version>-with-spark-<version>.jar

```

Memkind library also support DAX KMEM mode. Refer to [Kernel](https://github.com/memkind/memkind#kernel), this chapter will guide how to configure PMem as system RAM. Or [Memkind support for KMEM DAX option](https://pmem.io/2020/01/20/memkind-dax-kmem.html) for more details.

Please note that DAX KMEM mode need kernel version 5.x and memkind version 1.10 or above. If you choose KMEM mode, change memory manager from `pm` to `kmem` as below.
```
spark.sql.oap.cache.memory.manager           kmem
```

### Noevict cache

The noevict cache strategy is also supported in OAP based on the memkind library for PMem.

To use it in your workload, follow [prerequisites](#prerequisites) to set up PMem hardware correctly, also make sure memkind library installed. Then follow the configuration below.

```
# enable numa
spark.yarn.numa.enabled                                      true
spark.executorEnv.MEMKIND_ARENA_NUM_PER_KIND                 1
# for Parquet file format, enable binary cache
spark.sql.oap.parquet.binary.cache.enabled                   true 
# for ORC file format, enable binary cache
spark.sql.oap.orc.binary.cache.enabled                       true
spark.oap.cache.strategy                                     noevict 
spark.executor.sql.oap.cache.persistent.memory.initial.size  256g 

# Enable OAP extension in Spark
spark.sql.extensions              org.apache.spark.sql.OapExtensions

# absolute path of the jar on your working node, when in Yarn client mode
spark.files                       $HOME/miniconda2/envs/oapenv/oap_jars/oap-cache-<version>-with-spark-<version>.jar,$HOME/miniconda2/envs/oapenv/oap_jars/oap-common-<version>-with-spark-<version>.jar
# relative path of the jar, when in Yarn client mode
spark.executor.extraClassPath     ./oap-cache-<version>-with-spark-<version>.jar:./oap-common-<version>-with-spark-<version>.jar
# absolute path of the jar on your working node,when in Yarn client mode
spark.driver.extraClassPath       $HOME/miniconda2/envs/oapenv/oap_jars/oap-cache-<version>-with-spark-<version>.jar:$HOME/miniconda2/envs/oapenv/oap_jars/oap-common-<version>-with-spark-<version>.jar

```

### Vmemcache 

- Make sure [Vmemcache](https://github.com/pmem/vmemcache) library has been installed on every cluster worker node if vmemcache strategy is chosen for PMem cache. If you have finished [OAP-Installation-Guide](../../docs/OAP-Installation-Guide.md), vmemcache library will be automatically installed by Conda.
  
  Or you can follow the [build/install](./Developer-Guide.md#build-and-install-vmemcache) steps and make sure `libvmemcache.so` exist in `/lib64` directory on each worker node.
- To use it in your workload, follow [prerequisites](#prerequisites) to set up PMem hardware correctly.

#### Configure to enable PMem cache

Make the following configuration changes in `$SPARK_HOME/conf/spark-defaults.conf`.

```
# 2x number of your worker nodes
spark.executor.instances          6
# enable numa
spark.yarn.numa.enabled           true
# Enable OAP extension in Spark
spark.sql.extensions              org.apache.spark.sql.OapExtensions

# absolute path of the jar on your working node, when in Yarn client mode
spark.files                       $HOME/miniconda2/envs/oapenv/oap_jars/oap-cache-<version>-with-spark-<version>.jar,$HOME/miniconda2/envs/oapenv/oap_jars/oap-common-<version>-with-spark-<version>.jar
# relative path of the jar, when in Yarn client mode
spark.executor.extraClassPath     ./oap-cache-<version>-with-spark-<version>.jar:./oap-common-<version>-with-spark-<version>.jar
# absolute path of the jar on your working node,when in Yarn client mode
spark.driver.extraClassPath       $HOME/miniconda2/envs/oapenv/oap_jars/oap-cache-<version>-with-spark-<version>.jar:$HOME/miniconda2/envs/oapenv/oap_jars/oap-common-<version>-with-spark-<version>.jar

# for parquet file format, enable binary cache
spark.sql.oap.parquet.binary.cache.enabled                   true
# for ORC file format, enable binary cache
spark.sql.oap.orc.binary.cache.enabled                       true
# enable vmemcache strategy 
spark.oap.cache.strategy                                     vmem 
spark.executor.sql.oap.cache.persistent.memory.initial.size  256g 
# according to your cluster
spark.executor.sql.oap.cache.guardian.memory.size            10g
```
The `vmem` cache strategy is based on libvmemcache (buffer based LRU cache), which provides a key-value store API. Follow these steps to enable vmemcache support in Data Source Cache.

- `spark.executor.instances`: We suggest setting the value to 2X the number of worker nodes when NUMA binding is enabled. Each worker node runs two executors, each executor is bound to one of the two sockets, and accesses the corresponding PMem device on that socket.
- `spark.executor.sql.oap.cache.persistent.memory.initial.size`: It is configured to the available PMem capacity to be used as data cache per exectutor.
 
**NOTE**: If "PendingFiber Size" (on spark web-UI OAP page) is large, or some tasks fail with "cache guardian use too much memory" error, set `spark.executor.sql.oap.cache.guardian.memory.size ` to a larger number as the default size is 10GB. The user could also increase `spark.sql.oap.cache.guardian.free.thread.nums` or decrease `spark.sql.oap.cache.dispose.timeout.ms` to free memory more quickly.

#### Verify PMem cache functionality

- After finishing configuration, restart Spark Thrift Server for the configuration changes to take effect. Start at step 2 of the [Use DRAM Cache](./User-Guide.md#use-dram-cache) guide to verify that cache is working correctly.

- Verify NUMA binding status by confirming keywords like `numactl --cpubind=1 --membind=1` contained in executor launch command.

- Check PMem cache size by checking disk space with `df -h`.For `vmemcache` strategy, disk usage will reach the initial cache size once the PMem cache is initialized and will not change during workload execution. For `Guava/Noevict` strategies, the command will show disk space usage increases along with workload execution. 


## Index and Data Cache Separation

SQL Index and Data Source Cache now supports different cache strategies for DRAM and PMem. To optimize the cache media utilization, you can enable cache separation of data and index with same or different cache media. When Sharing same media, data cache and index cache will use different fiber cache ratio.


Here we list 4 different kinds of configuration for index/cache separation, if you choose one of them, please add corresponding configuration to `spark-defaults.conf`.
1. DRAM as cache media, `guava` strategy as index & data cache backend. 

```
spark.sql.oap.index.data.cache.separation.enabled       true
spark.oap.cache.strategy                                mix
spark.sql.oap.cache.memory.manager                      offheap
```
The rest configuration you can refer to  [Use DRAM Cache](./User-Guide.md#use-dram-cache) 

2. PMem as cache media, `external` strategy as index & data cache backend. 

```
spark.sql.oap.index.data.cache.separation.enabled       true
spark.oap.cache.strategy                                mix
spark.sql.oap.cache.memory.manager                      tmp
spark.sql.oap.mix.data.cache.backend                    external
spark.sql.oap.mix.index.cache.backend                   external

```
The rest configurations can refer to the configurations of [PMem Cache](./User-Guide.md#use-pmem-cache) and  [External cache](./User-Guide.md#Configuration-for-enabling-PMem-cache)

3. DRAM(`offheap`)/`guava` as `index` cache media and backend, PMem(`tmp`)/`external` as `data` cache media and backend. 

```
spark.sql.oap.index.data.cache.separation.enabled            true
spark.oap.cache.strategy                                     mix
spark.sql.oap.cache.memory.manager                           mix 
spark.sql.oap.mix.data.cache.backend                         external

# 2x number of your worker nodes
spark.executor.instances                                     6
# enable numa
spark.yarn.numa.enabled                                      true
spark.memory.offHeap.enabled                                 false

spark.sql.oap.dcpmm.free.wait.threshold                      50000000000
# according to your executor core number
spark.executor.sql.oap.cache.external.client.pool.size       10

# equal to the size of executor.memoryOverhead
spark.executor.sql.oap.cache.offheap.memory.size             50g
# according to the resource of cluster
spark.executor.memoryOverhead                                50g

# for ORC file format
spark.sql.oap.orc.binary.cache.enabled                       true
# for Parquet file format
spark.sql.oap.parquet.binary.cache.enabled                   true
```

4. DRAM(`offheap`)/`guava` as `index` cache media and backend, PMem(`pm`)/`guava` as `data` cache media and backend. 

```
spark.sql.oap.index.data.cache.separation.enabled            true
spark.oap.cache.strategy                                     mix
spark.sql.oap.cache.memory.manager                           mix 

# 2x number of your worker nodes
spark.executor.instances                                     6
# enable numa
spark.yarn.numa.enabled                                      true
spark.executorEnv.MEMKIND_ARENA_NUM_PER_KIND                 1
spark.memory.offHeap.enabled                                 false
# PMem capacity per executor
spark.executor.sql.oap.cache.persistent.memory.initial.size  256g
# Reserved space per executor
spark.executor.sql.oap.cache.persistent.memory.reserved.size 50g

# equal to the size of executor.memoryOverhead
spark.executor.sql.oap.cache.offheap.memory.size             50g
# according to the resource of cluster
spark.executor.memoryOverhead                                50g
# for ORC file format
spark.sql.oap.orc.binary.cache.enabled                       true
# for Parquet file format
spark.sql.oap.parquet.binary.cache.enabled                   true
```

## Cache Hot Tables

Data Source Cache also supports caching specific tables by configuring items according to actual situations, these tables are usually hot tables.

To enable caching specific hot tables, you can add the configuration below to `spark-defaults.conf`.
```
# enable table lists fiberCache
spark.sql.oap.cache.table.list.enabled          true
# Table lists using fiberCache actively
spark.sql.oap.cache.table.list                  <databasename>.<tablename1>;<databasename>.<tablename2>
```

## Column Vector Cache

This document above use **binary** cache for Parquet as example, cause binary cache can improve cache space utilization compared to ColumnVector cache. When your cluster memory resources are abundant enough, you can choose ColumnVector cache to spare computation time. 

To enable ColumnVector data cache for Parquet file format, you should add the configuration below to `spark-defaults.conf`.

```
# for parquet file format, disable binary cache
spark.sql.oap.parquet.binary.cache.enabled             false
# for parquet file format, enable ColumnVector cache
spark.sql.oap.parquet.data.cache.enabled               true
```

## Large Scale and Heterogeneous Cluster Support

***NOTE:*** Only works with `external cache`

OAP influences Spark to schedule tasks according to cache locality info. This info could be of large amount in a ***large scale cluster***, and how to schedule tasks in a ***heterogeneous cluster*** (some nodes with PMem, some without) could also be challenging.

We introduce an external DB to store cache locality info. If there's no cache available, Spark will fall back to schedule respecting HDFS locality.
Currently we support [Redis](https://redis.io/) as external DB service. Please [download and launch a redis-server](https://redis.io/download) before running Spark with OAP.

Please add the following configurations to `spark-defaults.conf`.
```
spark.sql.oap.external.cache.metaDB.enabled            true
# Redis-server address
spark.sql.oap.external.cache.metaDB.address            10.1.2.12
spark.sql.oap.external.cache.metaDB.impl               org.apache.spark.sql.execution.datasources.RedisClient
```
