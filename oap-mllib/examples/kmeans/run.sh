#!/usr/bin/env bash

# == User to customize the following environments ======= #

# Set user Spark and Hadoop home directory
export SPARK_HOME=/path/to/your/spark/home
export HADOOP_HOME=/path/to/your/hadoop/home
# Set user HDFS Root
export HDFS_ROOT=hdfs://your_hostname:8020
# Set user Intel MLlib Root directory
export OAP_MLLIB_ROOT=/path/to/your/OAP/oap-mllib
# Set IP and Port for oneCCL KVS, you can select any one of the worker nodes and set CCL_KVS_IP_PORT to its IP and Port
# IP can be got with `hostname -I`, if multiple IPs are returned, the first IP should be used. Port can be any available port.
# For example, if one of the worker IP is 192.168.0.1 and an available port is 51234. 
# CCL_KVS_IP_PORT can be set in the format of 192.168.0.1_51234
# Incorrectly setting this value will result in hanging when oneCCL initialize
export CCL_KVS_IP_PORT=192.168.0.1_51234

# Data file is from Spark examples' data (data/mllib/sample_kmeans_data.txt)
# This data file should be copied to HDFS hdfs://your_hostname:8020/user/<user_name>/data/sample_kmeans_data.txt
DATA_FILE=data/sample_kmeans_data.txt

# == User to customize Spark executor cores and memory == #

# User should check the requested resources are acturally allocated by cluster manager or Intel MLlib will behave incorrectly
SPARK_MASTER=yarn
SPARK_DRIVER_MEMORY=1G
SPARK_NUM_EXECUTORS=2
SPARK_EXECUTOR_CORES=1
SPARK_EXECUTOR_MEMORY=1G

SPARK_DEFAULT_PARALLELISM=$(expr $SPARK_NUM_EXECUTORS '*' $SPARK_EXECUTOR_CORES '*' 2)

# ======================================================= #

# Check envs
if [[ -z $SPARK_HOME ]]; then
    echo SPARK_HOME not defined!
    exit 1
fi

if [[ -z $HADOOP_HOME ]]; then
    echo HADOOP_HOME not defined!
    exit 1
fi

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# Target jar built
OAP_MLLIB_JAR_NAME=oap-mllib-1.0.0-with-spark-3.0.0.jar
OAP_MLLIB_JAR=$OAP_MLLIB_ROOT/mllib-dal/target/$OAP_MLLIB_JAR_NAME

# Use absolute path
SPARK_DRIVER_CLASSPATH=$OAP_MLLIB_JAR
# Use relative path
SPARK_EXECUTOR_CLASSPATH=./$OAP_MLLIB_JAR_NAME

APP_JAR=target/oap-mllib-examples-1.0.0-with-spark-3.0.0.jar
APP_CLASS=org.apache.spark.examples.ml.KMeansExample

/usr/bin/time -p $SPARK_HOME/bin/spark-submit --master $SPARK_MASTER -v \
    --num-executors $SPARK_NUM_EXECUTORS \
    --driver-memory $SPARK_DRIVER_MEMORY \
    --executor-cores $SPARK_EXECUTOR_CORES \
    --executor-memory $SPARK_EXECUTOR_MEMORY \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.default.parallelism=$SPARK_DEFAULT_PARALLELISM" \
    --conf "spark.sql.shuffle.partitions=$SPARK_DEFAULT_PARALLELISM" \
    --conf "spark.driver.extraClassPath=$SPARK_DRIVER_CLASSPATH" \
    --conf "spark.executor.extraClassPath=$SPARK_EXECUTOR_CLASSPATH" \
    --conf "spark.executorEnv.CCL_KVS_IP_PORT=$CCL_KVS_IP_PORT" \
    --conf "spark.shuffle.reduceLocality.enabled=false" \
    --conf "spark.network.timeout=1200s" \
    --conf "spark.task.maxFailures=1" \
    --jars $OAP_MLLIB_JAR \
    --class $APP_CLASS \
    $APP_JAR $DATA_FILE \
    2>&1 | tee KMeans-$(date +%m%d_%H_%M_%S).log
