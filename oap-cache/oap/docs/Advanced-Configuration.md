# Advanced Configuration

- [Additional Cache Strategies](#Additional-Cache-Strategies)  In addition to **vmem** cache strategy, Data Source Cache also supports 3 other cache strategies: **guava**, **noevict**  and **external cache**.
- [Index and Data Cache Separation](#Index-and-Data-Cache-Separation)  To optimize the cache media utilization, Data Source Cache supports cache separation of data and index, by using same or different cache media with DRAM and PMem.
- [Cache Hot Tables](#Cache-Hot-Tables)  Data Source Cache also supports caching specific tables according to actual situations, these tables are usually hot tables.
- [Column Vector Cache](#Column-Vector-Cache)  This document above use **binary** cache as example for Parquet file format, if your cluster memory resources is abundant enough, you can choose ColumnVector data cache instead of binary cache for Parquet to spare computation time.

## Additional Cache Strategies
Following table shows features of 4 cache strategies on PMem.

| guava | noevict | vmemcache | external cache |
| :----- | :----- | :----- | :-----|
| Use memkind lib to operate on PMem and guava cache strategy when data eviction happens. | Use memkind lib to operate on PMem and doesn't allow data eviction. | Use vmemcache lib to operate on PMem and LRU cache strategy when data eviction happens. | Use Plasma/dlmalloc to operate on PMem and LRU cache strategy when data eviction happens. |
| Need numa patch in Spark for better performance. | Need numa patch in Spark for better performance. | Need numa patch in Spark for better performance. | Doesn't need numa patch. |
| Suggest using 2 executors one node to keep aligned with PMem paths and numa nodes number. | Suggest using 2 executors one node to keep aligned with PMem paths and numa nodes number. | Suggest using 2 executors one node to keep aligned with PMem paths and numa nodes number. | Node-level cache so there are no limitation for executor number. |
| Cache data cleaned once executors exited. | Cache data cleaned once executors exited. | Cache data cleaned once executors exited. | No data loss when executors exit thus is friendly to dynamic allocation. But currently it has performance overhead than other cache solutions. |


- For cache solution `guava/noevict`, make sure [Memkind](https://github.com/memkind/memkind/tree/v1.10.1-rc2) library installed on every cluster worker node. Compile Memkind based on your system or directly place our pre-built binary of [libmemkind.so.0](https://github.com/Intel-bigdata/OAP/releases/download/v0.8.2-spark-3.0.0/libmemkind.so.0) for x86_64 bit CentOS Linux in the `/lib64/`directory of each worker node in cluster. Build and install step can refer to [build and install memkind](./Developer-Guide.md#build-and-install-memkind)

- For cache solution `vmemcahe/external` cache, make sure [Vmemcache](https://github.com/pmem/vmemcache) library has been installed on every cluster worker node if vmemcache strategy is chosen for PMem cache. You can follow the build/install steps from vmemcache website and make sure `libvmemcache.so` exist in `/lib64` directory in each worker node. You can download [vmemcache RPM package](https://github.com/Intel-bigdata/OAP/releases/download/v0.8.2-spark-3.0.0/libvmemcache-0.8..rpm), and install it by running `rpm -i libvmemcache*.rpm`. Build and install step can refer to [build and install vmemcache](./Developer-Guide.md#build-and-install-vmemcache)

- Data Source Cache use Plasma as a node-level external cache service, the benefit of using external cache is data could be shared across process boundaries.  [Plasma](http://arrow.apache.org/blog/2017/08/08/plasma-in-memory-object-store/) is a high-performance shared-memory object store, it's a component of [Apache Arrow](https://github.com/apache/arrow). We have modified Plasma to support PMem, and open source on [Intel-bigdata Arrow](https://github.com/Intel-bigdata/arrow/tree/oap-master) repo. Build and install step can refer to [build and install plasma](./Developer-Guide.md#build-and-install-plasma)
 
Or you can refer to [Developer-Guide](../../../docs/Developer-Guide.md), there is a shell script to help you install these dependencies automatically.

### Guava cache

Guava cache is based on memkind library, built on top of jemalloc and provides memory characteristics. To use it in your workload, follow [prerequisites](./User-Guide.md#prerequisites-1) to set up PMem hardware and memkind library correctly. Then follow bellow configurations.

**NOTE**: `spark.sql.oap.fiberCache.persistent.memory.reserved.size`: When we use PMem as memory through memkind library, some portion of the space needs to be reserved for memory management overhead, such as memory segmentation. We suggest reserving 20% - 25% of the available PMem capacity to avoid memory allocation failure. But even with an allocation failure, OAP will continue the operation to read data from original input data and will not cache the data block.

For Parquet file format, add these conf options:
```
spark.sql.oap.parquet.binary.cache.enabled        true
spark.sql.oap.fiberCache.memory.manager           pm 
spark.oap.cache.strategy                          guava
# PMem capacity per executor, according to your cluster
spark.sql.oap.fiberCache.persistent.memory.initial.size    256g
# Reserved space per executor
spark.sql.oap.fiberCache.persistent.memory.reserved.size   50g
spark.sql.extensions                              org.apache.spark.sql.OapExtensions
```
For Orc file format, add these conf options:
```
spark.sql.oap.orc.binary.cache.enable            true
spark.sql.oap.orc.enable                         true
spark.sql.oap.fiberCache.memory.manager          pm 
spark.oap.cache.strategy                         guava
# PMem capacity per executor, according to your cluster
spark.sql.oap.fiberCache.persistent.memory.initial.size    256g
# Reserved space per executor
spark.sql.oap.fiberCache.persistent.memory.reserved.size   50g
spark.sql.extensions                             org.apache.spark.sql.OapExtensions
```

Memkind library also support DAX KMEM mode. Refer [Kernel](https://github.com/memkind/memkind#kernel), this chapter will guide how to configure persistent memory as system ram. Or [Memkind support for KMEM DAX option](https://pmem.io/2020/01/20/memkind-dax-kmem.html) for more details.

Please note that DAX KMEM mode need kernel version 5.x and memkind version 1.10 or above. If you choose KMEM mode, change memory manager from `pm` to `kmem` as below.
```
spark.sql.oap.fiberCache.memory.manager           kmem
```

### Noevict cache

The noevict cache strategy is also supported in OAP based on the memkind library for PMem.

To apply noevict cache strategy in your workload, please follow [prerequisites](./User-Guide.md#prerequisites-1) to set up PMem hardware and memkind library correctly. Then follow bellow configurations.

For Parquet file format, add these conf options:
```
spark.sql.oap.parquet.binary.cache.enabled               true 
spark.oap.cache.strategy                                 noevict 
spark.sql.oap.fiberCache.persistent.memory.initial.size  256g 
```
For Orc file format, add these conf options:
```
spark.sql.oap.orc.binary.cache.enable                    true 
spark.oap.cache.strategy                                 noevict 
spark.sql.oap.fiberCache.persistent.memory.initial.size  256g 
```

### External cache using plasma


External cache strategy is implemented based on arrow/plasma library. For performance reason, we recommend using numa-patched spark 3.0.0. To use this strategy, follow [prerequisites](./User-Guide.md#prerequisites-1) to set up PMem hardware. Then install arrow rpm package which includes plasma library and executable file, copy arrow-plasma.jar to your ***SPARK_HOME/jars*** directory. Refer to below configurations to apply external cache strategy and start plasma service on each node and start your workload. (Currently web UI cannot display accurately, this is a known [issue](https://github.com/Intel-bigdata/OAP/issues/1579))

It's strongly advised to use [Linux device mapper](https://pmem.io/2018/05/15/using_persistent_memory_devices_with_the_linux_device_mapper.html) to interleave PMem across sockets and get maximum size for Plasma. You can follow these command to create or destroy interleaved PMem device:

```
# create interleaved PMem device
umount /mnt/pmem0
umount /mnt/pmem1
echo -e "0 $(( `sudo blockdev --getsz /dev/pmem0` + `sudo blockdev --getsz /dev/pmem0` )) striped 2 4096 /dev/pmem0 0 /dev/pmem1 0" | sudo dmsetup create striped-pmem
mkfs.ext4 -b 4096 -E stride=512 -F /dev/mapper/striped-pmem
mkdir -p /mnt/pmem
mount -o dax /dev/mapper/striped-pmem /mnt/pmem

# destroy interleaved PMem device
umount /mnt/pmem
dmsetup remove striped-pmem
mkfs.ext4 /dev/pmem0
mkfs.ext4 /dev/pmem1
mount -o dax /dev/pmem0 /mnt/pmem0
mount -o dax /dev/pmem1 /mnt/pmem1
```

For Parquet data format, add these conf options:


```
spark.sql.oap.parquet.binary.cache.enabled                 true 
spark.oap.cache.strategy                                   external
spark.sql.oap.dcpmm.free.wait.threshold                    50000000000
# according to your executor core number
spark.sql.oap.cache.external.client.pool.size              10
```

For Orc file format, add these conf options:

```
spark.sql.oap.orc.binary.cache.enable                      true 
spark.oap.cache.strategy                                   external
spark.sql.oap.dcpmm.free.wait.threshold                    50000000000
# according to your executor core number
spark.sql.oap.cache.external.client.pool.size              10
```

- Start plasma service manually

plasma config parameters:  
 ```
 -m  how much Bytes share memory plasma will use
 -s  Unix Domain sockcet path
 -d  Pmem directory
 ```

You can start plasma service on each node as following command, and then you can run your workload.

```
plasma-store-server -m 15000000000 -s /tmp/plasmaStore -d /mnt/pmem  
```

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
     "launch_command": "plasma-store-server -m 15000000000 -s /tmp/plasmaStore -d /mnt/pmem",
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

## Index and Data Cache Separation

Data Source Cache now supports different cache strategies for DRAM and PMem. To optimize the cache media utilization, you can enable cache separation of data and index with same or different cache media. When Sharing same media, data cache and index cache will use different fiber cache ratio.


Here we list 4 different kinds of configs for index/cache separation, if you choose one of them, please add corresponding configs to `spark-defaults.conf`.
1. DRAM as cache media, `guava` strategy as index & data cache backend. 

```
spark.sql.oap.index.data.cache.separation.enable        true
spark.oap.cache.strategy                                mix
spark.sql.oap.fiberCache.memory.manager                 offheap
```
The rest configurations can refer to the configurations of  [Use DRAM Cache](./User-Guide.md#use-dram-cache) 

2. PMem as cache media, `vmem` strategy as index & data cache backend. 

```
spark.sql.oap.index.data.cache.separation.enable        true
spark.oap.cache.strategy                                mix
spark.sql.oap.fiberCache.memory.manager                 tmp
spark.sql.oap.mix.data.cache.backend                    vmem
spark.sql.oap.mix.index.cache.backend                   vmem

```
The rest configurations can refer to the configurations of [PMem Cache](./User-Guide.md#use-pmem-cache) and  [Vmemcache cache](./User-Guide.md#configure-to-enable-pmem-cache)

3. DRAM(`offheap`)/`guava` as `index` cache media and backend, PMem(`tmp`)/`vmem` as `data` cache media and backend. 

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
# PMem capacity per executor
spark.sql.oap.fiberCache.persistent.memory.initial.size  256g
# according to your cluster
spark.sql.oap.cache.guardian.memory.size                 10g

# equal to the size of executor.memoryOverhead
spark.sql.oap.fiberCache.offheap.memory.size   50g
# according to the resource of cluster
spark.executor.memoryOverhead                  50g

# for orc file format
spark.sql.oap.orc.binary.cache.enable            true
# for Parquet file format
spark.sql.oap.parquet.binary.cache.enabled       true
```

4. DRAM(`offheap`)/`guava` as `index` cache media and backend, PMem(`pm`)/`guava` as `data` cache media and backend. 

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
# PMem capacity per executor
spark.sql.oap.fiberCache.persistent.memory.initial.size  256g
# Reserved space per executor
spark.sql.oap.fiberCache.persistent.memory.reserved.size 50g

# equal to the size of executor.memoryOverhead
spark.sql.oap.fiberCache.offheap.memory.size   50g
# according to the resource of cluster
spark.executor.memoryOverhead                  50g
# for ORC file format
spark.sql.oap.orc.binary.cache.enable          true
# for Parquet file format
spark.sql.oap.parquet.binary.cache.enabled      true
```

## Cache Hot Tables

Data Source Cache also supports caching specific tables by configuring items according to actual situations, these tables are usually hot tables.

To enable caching specific hot tables, you can add below configurations to `spark-defaults.conf`.
```
# enable table lists fiberCache
spark.sql.oap.fiberCache.table.list.enable      true
# Table lists using fiberCache actively
spark.sql.oap.fiberCache.table.list             <databasename>.<tablename1>;<databasename>.<tablename2>
```

## Column Vector Cache

This document above use **binary** cache for Parquet as example, cause binary cache can improve cache space utilization compared to ColumnVector cache. When your cluster memory resources are abundant enough, you can choose ColumnVector cache to spare computation time. 

To enable ColumnVector data cache for Parquet file format, you should add below configurations to `spark-defaults.conf`.

```
# for parquet file format, disable binary cache
spark.sql.oap.parquet.binary.cache.enabled      false
# for parquet file format, enable ColumnVector cache
spark.sql.oap.parquet.data.cache.enable         true
```

