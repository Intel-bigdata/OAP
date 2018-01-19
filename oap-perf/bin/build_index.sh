#!/bin/bash

# first source config
. ./benchmark.config

hadoop fs -rm -r /user/oap/oaptest/oap-0."$oapVersionNum".0/tpcds/tpcds"$dataScale"/oap/store_sales/*.index
hadoop fs -rm -r /user/oap/oaptest/oap-0."$oapVersionNum".0/tpcds/tpcds"$dataScale"/parquet/store_sales/*.index

${sparkPath}/bin/spark-submit \
	--class ${packagePath}.IndexBuilder \
	--master yarn \
	--deploy-mode client \
	$benchmarkJarPath \
	$oapVersionNum \
	$benchmarkFormats \
	$dataScale \
	$indexFlag \
	1 > $indexInfoPath
