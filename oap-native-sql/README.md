# Spark Native SQL Engine

A Native Engine for Spark SQL with vectorized SIMD optimizations

## Introduction

![Overview](/oap-native-sql/resource/nativesql_arch.png)

Spark SQL works very well with structured row-based data. It used WholeStageCodeGen to improve the performance by Java JIT code. However Java JIT is usually not working very well on utilizing latest SIMD instructions, especially under complicated queries. [Apache Arrow](https://arrow.apache.org/) provided CPU-cache friendly columnar in-memory layout, its SIMD optimized kernels and LLVM based SQL engine Gandiva are also very efficient. Native SQL Engine used these technoligies and brought better performance to Spark SQL.

## Key Features

### Apache Arrow formatted intermediate data among Spark operator

![Overview](/oap-native-sql/resource/columnar.png)

With [Spark 27396](https://issues.apache.org/jira/browse/SPARK-27396) its possible to pass a RDD of Columnarbatch to operators. We implemented this API with Arrow columnar format.

### Apache Arrow based Native Readers for Parquet and other formats

![Overview](/oap-native-sql/resource/dataset.png)

A native parquet reader was developed to speed up the data loading. it's based on Apache Arrow Dataset. For details please check [Arrow Data Source](../oap-data-source/README.md)

### Apache Arrow Compute/Gandiva based operators

![Overview](/oap-native-sql/resource/kernel.png)

We implemented common operators based on Apache Arrow Compute and Gandiva. The SQL expression was compiled to one expression tree with protobuf and passed to native kernels. The native kernels will then evaluate the these expressions based on the input columnar batch.

### Native Columnar Shuffle Operator with efficient compression support

![Overview](/oap-native-sql/resource/shuffle.png)

We implemented columnar shuffle to improve the shuffle performance. With the columnar layout we could do very efficient data compression for different data format.

## Testing

Check out the detailed [installation/testing guide](/oap-native-sql/resource/installation.md) for quick testing

## Performance

The chart in below shows the results by using Decision Support Benchmark1 and Decision Support Benchmark2.
All the testing environment is using 1 master + 3 workers and Intel(r) Xeon(r) Gold 6252 CPU|384GB memory|NVMe SSD x3 per node with 1.5TB dataset.
Decision Support Benchmark1 is a query set modified from [TPC-H benchmark](http://tpc.org/tpch/default5.asp). We change Decimal to Double since Decimal hasn't been supported in OAP v1.0-Native SQL Engine.
Overall, the result shows a 1.49X performance speed up from OAP v1.0-Native SQL Engine comparing to Vanilla SPARK 3.0.0.
We also put the detail result by queries, most of queries in Decision Support Benchmark1 can take the advantages from Native SQL Engine. The performance boost ratio may depend on the individual query.

![Overview](/oap-native-sql/resource/decision_support_bench1_result_in_total.png)

![Overview](/oap-native-sql/resource/decision_support_bench1_result_by_query.png)

Decision Support Benchmark2 is a query set modified from [TPC-DS benchmark](http://tpc.org/tpcds/default5.asp). We change Decimal to Doubel since Decimal hasn't been supported in OAP v1.0-Native SQL Engine.
We pick up 10 queries can be fully supported in OAP v1.0-Native SQL Engine and the result shows a 1.26X performance speed up comparing to Vanilla SPARK 3.0.0.

![Overview](/oap-native-sql/resource/decision_support_bench2_result_in_total.png)

![Overview](/oap-native-sql/resource/decision_support_bench2_result_by_query.png)

## Contact

chendi.xue@intel.com
weiting.chen@intel.com
binwei.yang@intel.com
