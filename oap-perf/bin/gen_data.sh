#!/bin/sh

# first source config
. ./benchmark.config

${sparkPath}/bin/spark-submit \
	--class ${packagePath}.DataGen \
	--master yarn \
	--deploy-mode client \
	$benchmarkJarPath \
	$oapVersionNum \
	$dataScale \
	$testTrie \
	$dataPartitions
