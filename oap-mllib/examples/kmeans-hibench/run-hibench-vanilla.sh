#!/usr/bin/env bash

# == User to customize the following environments ======= #

# Set user Spark and Hadoop home directory
export SPARK_HOME=/path/to/your/spark/home
export HADOOP_HOME=/path/to/your/hadoop/home
# Set user HDFS Root
export HDFS_ROOT=hdfs://your_hostname:8020

# == User to customize Spark executor cores and memory == #

SPARK_MASTER=yarn
SPARK_DRIVER_MEMORY=8G
SPARK_NUM_EXECUTORS=18
SPARK_EXECUTOR_CORES=5
SPARK_EXECUTOR_MEMORY_OVERHEAD=25G
SPARK_EXECUTOR_MEMORY=50G

SPARK_DEFAULT_PARALLELISM=$(expr $SPARK_NUM_EXECUTORS '*' $SPARK_EXECUTOR_CORES '*' 2)

# ======================================================= #

# for log suffix
SUFFIX=$( basename -s .sh "${BASH_SOURCE[0]}" )

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

APP_JAR=target/oap-mllib-examples-0.9.0-with-spark-3.0.0.jar
APP_CLASS=com.intel.hibench.sparkbench.ml.DenseKMeansDS

K=200
INIT_MODE=Random
MAX_ITERATION=20
INPUT_HDFS=$HDFS_ROOT/HiBench/Kmeans/Input/samples

/usr/bin/time -p $SPARK_HOME/bin/spark-submit --master $SPARK_MASTER -v \
    --num-executors $SPARK_NUM_EXECUTORS \
    --driver-memory $SPARK_DRIVER_MEMORY \
    --executor-cores $SPARK_EXECUTOR_CORES \
    --executor-memory $SPARK_EXECUTOR_MEMORY \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.default.parallelism=$SPARK_DEFAULT_PARALLELISM" \
    --conf "spark.sql.shuffle.partitions=$SPARK_DEFAULT_PARALLELISM" \
    --class $APP_CLASS \
    $APP_JAR \
    -k $K --initMode $INIT_MODE --numIterations $MAX_ITERATION $INPUT_HDFS \
    2>&1 | tee KMeansHiBench-$SUFFIX-$(date +%m%d_%H_%M_%S).log
