#!/usr/bin/env bash

# == User to customize the following environments ======= #

# Set user Spark and Hadoop home directory
export SPARK_HOME=/path/to/your/spark/home
export HADOOP_HOME=/path/to/your/hadoop/home
# Set user HDFS Root
export HDFS_ROOT=hdfs://your_hostname:8020
# Set user Intel MLlib Root directory
export OAP_MLLIB_ROOT=/path/to/your/OAP/oap-mllib
# Set IP_Port to one of the worker nodes for oneCCL
# If you have multiple IPs for nodes, use first IP returned from `hostname -I`
export CCL_KVS_IP_PORT=192.168.0.1_51234

# Data file is from Spark Examples (data/mllib/sample_kmeans_data.txt), the data file should be copied to HDFS
DATA_FILE=data/sample_kmeans_data.txt

# == User to customize Spark executor cores and memory == #

SPARK_MASTER=yarn
SPARK_DRIVER_MEMORY=1G
SPARK_NUM_EXECUTORS=2
SPARK_EXECUTOR_CORES=1
SPARK_EXECUTOR_MEMORY=1G

SPARK_DEFAULT_PARALLELISM=$(expr $SPARK_NUM_EXECUTORS '*' $SPARK_EXECUTOR_CORES '*' 2)

# ======================================================= #

# Check env
if [[ -z $SPARK_HOME ]]; then
    echo SPARK_HOME not defined!
    exit 1
fi

if [[ -z $HADOOP_HOME ]]; then
    echo HADOOP_HOME not defined!
    exit 1
fi

if [[ -z $DAALROOT ]]; then
    echo DAALROOT not defined!
    exit 1
fi

if [[ -z $TBBROOT ]]; then
    echo TBBROOT not defined!
    exit 1
fi

if [[ -z $CCL_ROOT ]]; then
    echo CCL_ROOT not defined!
    exit 1
fi

# Target jar built
OAP_MLLIB_JAR_NAME=oap-mllib-0.9.0-with-spark-3.0.0.jar
OAP_MLLIB_JAR=$OAP_MLLIB_ROOT/mllib-dal/target/$OAP_MLLIB_JAR_NAME

# Use absolute path
SPARK_DRIVER_CLASSPATH=$OAP_MLLIB_JAR
# Use relative path
SPARK_EXECUTOR_CLASSPATH=./$OAP_MLLIB_JAR_NAME

APP_PY=kmeans-pyspark.py

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
    --jars $OAP_MLLIB_JAR \
    $APP_PY $DATA_FILE \
    2>&1 | tee KMeans-$(date +%m%d_%H_%M_%S).log
