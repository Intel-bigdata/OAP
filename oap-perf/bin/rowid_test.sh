#!/bin/sh
bin/spark-submit \
	--class ${packagePath}.RowIdTest \
	--master yarn \
	--deploy-mode client \
	$benchmarkJarPath \
	$oapVersionNum
