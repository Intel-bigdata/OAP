# RDD Cache PMem Extension

## Contents
- [Introduction](#introduction)
- [User Guide](#userguide)

## Introduciton

OAP Spark supports RDD Cache with Optane PMem. Spark has various storage levels serving for different purposes including memory and disk.

PMem storage level is added to support a new tier for storage level besides memory and disk.

Using PMem library to access Optane PMem can help to avoid the overhead from disk.

Large capacity and high I/O performance of PMem shows better performance than tied DRAM and disk solution under the same cost.

## User Guide

### Installation
We have provided a Conda package which will automatically install dependencies needed by OAP, you can refer to [OAP-Installation-Guide](../docs/OAP-Installation-Guide.md) for more information. If you have finished [OAP-Installation-Guide](../docs/OAP-Installation-Guide.md),you needn't compile and install Memkind, and you can find compiled OAP jars in `$HOME/miniconda2/envs/oapenv/oap_jars`.

### Prerequisites

The following are required to configure OAP to use PMem cache in AppDirect mode.
- PMem hardware is successfully deployed on each node in cluster.
- Directories exposing PMem hardware on each socket. For example, on a two socket system the mounted PMem directories should appear as `/mnt/pmem0` and `/mnt/pmem1`. Correctly installed PMem must be formatted and mounted on every cluster worker node.

   ```
   // use ipmctl command to show topology and dimm info of PMem
   ipmctl show -topology
   ipmctl show -dimm
   // provision PMem in app direct mode
   ipmctl create -goal PersistentMemoryType=AppDirect
   // reboot system to make configuration take affect
   reboot
   // check capacity provisioned for app direct mode(AppDirectCapacity)
   ipmctl show -memoryresources
   // show the PMem region information
   ipmctl show -region
   // create namespace based on the region, multi namespaces can be created on a single region
   ndctl create-namespace -m fsdax -r region0
   ndctl create-namespace -m fsdax -r region1
   // show the created namespaces
   fdisk -l
   // create and mount file system
   echo y | mkfs.ext4 /dev/pmem0
   echo y | mkfs.ext4 /dev/pmem1
   mkdir -p /mnt/pmem0
   mkdir -p /mnt/pmem1 
   mount -o dax /dev/pmem0 /mnt/pmem0
   mount -o dax /dev/pmem1 /mnt/pmem1
   ```

   In this case file systems are generated for 2 numa nodes, which can be checked by "numactl --hardware". For a different number of numa nodes, a corresponding number of namespaces should be created to assure correct file system paths mapping to numa nodes.


- Make sure [Memkind](https://github.com/memkind/memkind/tree/v1.10.1-rc2) library installed on every cluster worker node. Compile Memkind based on your system or directly place our pre-built binary of [libmemkind.so.0](https://github.com/Intel-bigdata/OAP/releases/download/v0.9.0-spark-3.0.0/libmemkind.so.0) for x86 64bit CentOS Linux in the `/lib64/`directory of each worker node in cluster.
   The Memkind library depends on `libnuma` at the runtime, so it must already exist in the worker node system.
   Build the latest memkind lib from source:

   ```
   git clone -b v1.10.1-rc2 https://github.com/memkind/memkind
   cd memkind
   ./autogen.sh
   ./configure
   make
   make install
   ```

- For KMem Dax mode, we need to configure PMem as system ram. Kernel 5.1 or above is required to this mode.

   ```
   daxctl migrate-device-model
   ndctl create-namespace --mode=devdax --map=mem
   ndctl list
   daxctl reconfigure-device dax0.0 --mode=system-ram
   daxctl reconfigure-device dax1.0 --mode=system-ram
   daxctl reconfigure-device daxX.Y --mode=system-ram
   ```

Refer [Memkind KMem](https://github.com/memkind/memkind#kernel) for details.


### Compiling

To build oap spark and oap common, you can run below commands:
```
cd ${OAP_CODE_HOME}
mvn clean package -Ppersistent-memory -DskipTests
```
You will find jar files under oap-common/target and oap-spark/target.

### Configuration

To enable rdd cache on Intel Optane PMem, you need add the following configurations to `spark-defaults.conf`
```
spark.memory.pmem.initial.path [Your Optane PMem paths seperate with comma]
spark.memory.pmem.initial.size [Your Optane PMem size in GB]
spark.memory.pmem.usable.ratio [from 0 to 1, 0.85 is recommended]
spark.yarn.numa.enabled true
spark.yarn.numa.num [Your numa node number]
spark.memory.pmem.mode [AppDirect | KMemDax]

spark.files                       file://${PATH_TO_OAP_SPARK_JAR}/oap-spark-<version>-with-spark-<version>.jar,file://${{PATH_TO_OAP_COMMON_JAR}/oap-common-<version>-with-spark-<version>.jar
spark.executor.extraClassPath     ./oap-spark-<version>-with-spark-<version>.jar:./oap-common-<version>-with-spark-<version>.jar
spark.driver.extraClassPath       file://${PATH_TO_OAP_SPARK_JAR}/oap-spark-<version>-with-spark-<version>.jar:file://${{PATH_TO_OAP_COMMON_JAR}/oap-common-<version>-with-spark-<version>.jar
```

### Use Optane PMem to cache data

There's a new StorageLevel: PMEM_AND_DISK being added to cache data to Optane PMem, at the places you previously cache/persist data to memory, use PMEM_AND_DISK to substitute the previous StorageLevel, data will be cached to Optane PMem.
```
persist(StorageLevel.PMEM_AND_DISK)
```

### Run K-means benchmark

You can use [Hibench](https://github.com/Intel-bigdata/HiBench) to run K-means workload:

After you Build Hibench, then follow Run SparkBench documentation. Here are some tips besides this documentation you need to notice.
Follow the documentation to configure these 4 files:
```
HiBench/conf/hadoop.conf
HiBench/conf/hibench.conf
HiBench/conf/spark.conf
HiBench/conf/workloads/ml/kmeans.conf
```
Note that you need add `hibench.kmeans.storage.level  PMEM_AND_DISK` to `kmeans.conf`, which can enable both PMem and Disk to cache data.
If you completed [OAP-Installation-Guide](../docs/OAP-Installation-Guide.md), you also need add the following configs to `spark.conf`
```
spark.executorEnv.LD_LIBRARY_PATH   $HOME/miniconda2/envs/oapenv/lib
spark.executor.extraLibraryPath     $HOME/miniconda2/envs/oapenv/lib
spark.driver.extraLibraryPath       $HOME/miniconda2/envs/oapenv/lib

```
 
Then you can run the following 2 commands to run K-means workloads:
```
bin/workloads/ml/kmeans/prepare/prepare.sh
bin/workloads/ml/kmeans/spark/run.sh
```
Then you can find the log as below:
```
patching args=
Parsing conf: /home/wh/HiBench/conf/hadoop.conf
Parsing conf: /home/wh/HiBench/conf/hibench.conf
Parsing conf: /home/wh/HiBench/conf/spark.conf
Parsing conf: /home/wh/HiBench/conf/workloads/ml/kmeans.conf
probe sleep jar: /opt/Beaver/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.3-tests.jar
start ScalaSparkKmeans bench
hdfs rm -r: /opt/Beaver/hadoop/bin/hadoop --config /opt/Beaver/hadoop/etc/hadoop fs -rm -r -skipTrash hdfs://vsr219:9000/HiBench/Kmeans/Output
rm: `hdfs://vsr219:9000/HiBench/Kmeans/Output': No such file or directory
hdfs du -s: /opt/Beaver/hadoop/bin/hadoop --config /opt/Beaver/hadoop/etc/hadoop fs -du -s hdfs://vsr219:9000/HiBench/Kmeans/Input
Export env: SPARKBENCH_PROPERTIES_FILES=/home/wh/HiBench/report/kmeans/spark/conf/sparkbench/sparkbench.conf
Export env: HADOOP_CONF_DIR=/opt/Beaver/hadoop/etc/hadoop
Submit Spark job: /opt/Beaver/spark/bin/spark-submit  --properties-file /home/wh/HiBench/report/kmeans/spark/conf/sparkbench/spark.conf --class com.intel.hibench.sparkbench.ml.DenseKMeans --master yarn-client --num-executors 2 --executor-cores 45 --executor-memory 100g /home/wh/HiBench/sparkbench/assembly/target/sparkbench-assembly-8.0-SNAPSHOT-dist.jar -k 10 --numIterations 5 --storageLevel PMEM_AND_DISK hdfs://vsr219:9000/HiBench/Kmeans/Input/samples
20/07/03 09:07:49 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-43459116-f4e3-4fe1-bb29-9bca0afa5286
finish ScalaSparkKmeans bench
```

Open the Spark History Web UI and go to the Storage tab page to verify the cache metrics.

### Limitations

For the scenario that data will exceed the block cache capacity. Memkind 1.9.0 and kernel 4.18 is recommended to avoid the unexpected issue.


### How to contribute

Currently, OAP Spark packages includes all Spark changed files.

* MemoryMode.java
* MemoryManager.scala
* StorageMemoryPool.scala
* UnifiedMemoryManager.scala
* MemoryStore.scala
* BlockManager.scala
* StorageLevel.scala
* TestMemoryManager

Please make sure your code change in above source code will not break current function.

The files from this package should avoid depending on other OAP module except OAP-Common.
