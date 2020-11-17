#!/usr/bin/env bash

WORK_DIR="$( cd $( dirname "${BASH_SOURCE[0]}" ) && pwd )"

DAAL_JAR=${ONEAPI_ROOT}/dal/latest/lib/onedal.jar

if [ ! -f "$DAAL_JAR" ]; then
    echo $DAAL_JAR does not exist!
    exit 1
fi

if [[ ! -e "$SPARK_HOME" ]]; then
	echo $SPARK_HOME does not exist!
    exit 1	
fi

javah -d $WORK_DIR/javah -classpath "$WORK_DIR/../../../target/classes:$DAAL_JAR:$SPARK_HOME/jars/*" -force \
    org.apache.spark.ml.util.OneCCL$ \
    org.apache.spark.ml.util.OneDAL$ \
    org.apache.spark.ml.clustering.KMeansDALImpl \
    org.apache.spark.ml.feature.PCADALImpl
