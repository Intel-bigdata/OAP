# Spark Configurations for Native SQL Engine

Add below configuration to spark-defaults.conf

```
##### Columnar Process Configuration

spark.sql.sources.useV1SourceList avro
spark.sql.join.preferSortMergeJoin false
spark.sql.extensions com.intel.oap.ColumnarPlugin
spark.shuffle.manager org.apache.spark.shuffle.sort.ColumnarShuffleManager

# note native sql engine depends on arrow data source
spark.driver.extraClassPath $HOME/miniconda2/envs/oapenv/oap_jars/spark-columnar-core-1.0.0-jar-with-dependencies.jar:$HOME/miniconda2/envs/oapenv/oap_jars/spark-arrow-datasource-standard-1.0.0-jar-with-dependencies.jar
spark.executor.extraClassPath $HOME/miniconda2/envs/oapenv/oap_jars/spark-columnar-core-1.0.0-jar-with-dependencies.jar:$HOME/miniconda2/envs/oapenv/oap_jars/spark-arrow-datasource-standard-1.0.0-jar-with-dependencies.jar

spark.executorEnv.LIBARROW_DIR      $HOME/miniconda2/envs/oapenv
spark.executorEnv.CC                $HOME/miniconda2/envs/oapenv/bin/gcc
######
```

Before you start spark, you must use below command to add some environment variables.
```shell script
export CC=$HOME/miniconda2/envs/oapenv/bin/gcc
export LIBARROW_DIR=$HOME/miniconda2/envs/oapenv/
```

About spark-arrow-datasource.jar, you can refer [Unified Arrow Data Source ](../../oap-data-source/arrow/README.md).
