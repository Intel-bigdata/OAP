#!/bin/bash

# for profiler use
# influx -host 10.1.2.130 << eof
# drop database profiler;
# create database profiler;
# eof

# first source config
. ./benchmark.config


${sparkPath}/bin/spark-submit \
	--class ${packagePath}.BenchmarkTest \
	--master yarn \
	--deploy-mode client \
	$benchmarkJarPath \
	$oapVersionNum \
	$benchmarkFormats \
	$dataScale \
	$useIndexes \
	$indexFlag \
	$testOapStrategy \
	$enableStatistic \
	$benchmarkTestTimes \
	> $benchmarkResPath
# for profiler use
# --conf "spark.executor.extraJavaOptions=-javaagent:statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar=server=10.1.2.130<Plug>PeepOpenort=8086,reporter=InfluxDBReporter,                database=profiler,username=profiler<Plug>PeepOpenassword=profiler<Plug>PeepOpenrefix="$format",tagMapping="$format \
	        #--jars /home/oap/oap/statsd-jvm-profiler-2.1.1-SNAPSHOT-jar-with-dependencies.jar \

