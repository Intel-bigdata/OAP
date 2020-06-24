# RDD Cache PMem Extension
Please add feature description here.

## Contents
- [Introduction](#introduction)
- [User Guide](#userguide)

## Introduciton
Please add the detailed introduction and conceptual architecture overview here.

## User Guide
### Prerequisites

Before getting start with storage extension with Optane PMem, your machine should have Intel Optane PMem setup and you should have memkind being installed. For memkind installation, please refer [memkind webpage](https://github.com/memkmemkindind/).

Please refer to documentation at ["Quick Start Guide: Provision Intel® Optane™ DC Persistent Memory"](https://software.intel.com/en-us/articles/quick-start-guide-configure-intel-optane-dc-persistent-memory-on-linux) for detailed to setup Optane PMem with App Direct Mode.

### Configuration

To enable rdd cache on Intel Optane PMem, you need add the following configurations:
```
spark.memory.pmem.initial.path [Your Optane PMem paths seperate with comma]
spark.memory.pmem.initial.size [Your Optane PMem size in GB]
spark.yarn.numa.enabled true
spark.yarn.numa.num [Your numa node number]

spark.files                       file://${{PATH_TO_OAP_SPARK_JAR}/oap-spark-${VERSION}.jar,file://${{PATH_TO_OAP_COMMON_JAR}/oap-common-${VERSION}.jar
spark.executor.extraClassPath     ./oap-spark-${VERSION}.jar:./oap-common-${VERSION}.jar
spark.driver.extraClassPath       file://${{PATH_TO_OAP_SPARK_JAR}/oap-spark-${VERSION}.jar:file://${{PATH_TO_OAP_COMMON_JAR}/oap-common-${VERSION}.jar
```

### Use Optane PMem to cache data

There's a new StorageLevel: PMEM_AND_DISK being added to cache data to Optane PMem, at the places you previously cache/persist data to memory, use PMEM_AND_DISK to substitute the previous StorageLevel, data will be cached to Optane PMem.
```
persist(StorageLevel.PMEM_AND_DISK)
```

### Run K-means benchmark

You can use [Hibench](https://github.com/Intel-bigdata/HiBench) to run K-means workload:

### Limitations

For the scenario that data will exceed the block cache capacity. Memkind 1.9.0 and kernel 4.18 is recommended to avoid the unexpected issue.


### How to contribute

OAP Spark packages includes all Spark changed files. All codes are directly copied from
https://github.com/Intel-bigdata/Spark. Please make sure all your changes are committed to the
repository above. Otherwise, your change will be override by others.

The files from this package should avoid depending on other OAP module except OAP-Common.

All Spark source code changes are tracked in dev/changes_list/spark_changed_files

All changed files are ordered by file name.

You can execute the script dev/Apply_Spark_changes.sh with the specified Spark source directories
and OAP source directories accordingly.
