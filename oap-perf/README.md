# Benchmark guide of OAP
This module serves as the benchmark test for OAP-0.3.0 features. 

## Prerequisites
This module uses SBT-1.0.2 to build itself. Before running "sbt assembly" to build the module, please set the repositories mirrors or the downloading can be very slow.

## Workflow
* Firstly please run "sbt assembly" to build the necessary jar file for testing under the oap-perf directory.
* Setting bin/benchmark.config for all settings of benchmark workflows.
* Run bin/runDataGen.sh to generate tpcds data in hdfs.
* Run bin/runCreateDatabase.sh to transform table (e.g, remove null values, scale values of index attributes) and generate table.
* Run bin/runIndexBuild.sh to generate index.
* Run bin/runBenchmarkTest.sh to generate benchmark results. Notice as we use clear cache commands in scala code, which needs sudo operation, you'd better run sudo bin/runBenchmarkTest.sh. Though the command won't throw execption if not executed as sudoers, there maybe impact on final results.

Normally you just run the first 5 steps for preparation and don't need to run them again. Besides, please run these steps under oap-perf directory or at least from a fixed directory. The reason is Spark creates a metastore_db directory under your execution directory to store database catalog. 
