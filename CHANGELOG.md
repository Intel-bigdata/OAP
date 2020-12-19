# Change log
Generated on 2020-12-19

## Release 1.0.0

### Features
|||
|:---|:---|
|[#1588](https://github.com/Intel-bigdata/OAP/issues/1588)|[OAP-CACHE] Make Parquet file splitable|
|[#1337](https://github.com/Intel-bigdata/OAP/issues/1337)|[oap-cacnhe] Discard OAP data format|
|[#1679](https://github.com/Intel-bigdata/OAP/issues/1679)|[OAP-CACHE]Remove the code related to reading and writing OAP data format|
|[#1680](https://github.com/Intel-bigdata/OAP/issues/1680)|[OAP-CACHE]Decouple spark code includes FileFormatDataWriter, FileFormatWriter and OutputWriter|
|[#1846](https://github.com/Intel-bigdata/OAP/issues/1846)|[oap-native-sql] spark sql unit test|
|[#1811](https://github.com/Intel-bigdata/OAP/issues/1811)|[OAP-cache]provide one-step starting scripts like plasma-sever redis-server|
|[#1519](https://github.com/Intel-bigdata/OAP/issues/1519)|[oap-native-sql] upgrade cmake|
|[#1835](https://github.com/Intel-bigdata/OAP/issues/1835)|[oap-native-sql] Support ColumnarBHJ to Build and Broadcast HashRelation in driver side|
|[#1848](https://github.com/Intel-bigdata/OAP/issues/1848)|[OAP-CACHE]Decouple spark code include OneApplicationResource.scala|
|[#1824](https://github.com/Intel-bigdata/OAP/issues/1824)|[OAP-CACHE]Decouple spark code includes DataSourceScanExec.scala.|
|[#1838](https://github.com/Intel-bigdata/OAP/issues/1838)|[OAP-CACHE]Decouple spark code includes VectorizedColumnReader.java, VectorizedPlainValuesReader.java, VectorizedRleValuesReader.java and OnHeapColumnVector.java|
|[#1839](https://github.com/Intel-bigdata/OAP/issues/1839)|[oap-native-sql] Add prefetch to columnar shuffle split|
|[#1756](https://github.com/Intel-bigdata/OAP/issues/1756)|[Intel MLlib] Add Kmeans "tolerance" support and test cases|
|[#1818](https://github.com/Intel-bigdata/OAP/issues/1818)|[OAP-Cache]Make Spark webUI OAP Tab more user friendly|
|[#1831](https://github.com/Intel-bigdata/OAP/issues/1831)|[oap-native-sql] ColumnarWindow: Support reusing same window spec in multiple functions|
|[#1765](https://github.com/Intel-bigdata/OAP/issues/1765)|[oap-native-sql] Support WSCG in nativesql|
|[#1517](https://github.com/Intel-bigdata/OAP/issues/1517)|[oap-native-sql] implement SortMergeJoin|
|[#1654](https://github.com/Intel-bigdata/OAP/issues/1654)|[oap-native-sql] Columnar shuffle TPCDS enabling|
|[#1700](https://github.com/Intel-bigdata/OAP/issues/1700)|[oap-native-sql] Support inside join condition project|
|[#1717](https://github.com/Intel-bigdata/OAP/issues/1717)|[oap-native-sql] support null in columnar literal and subquery|
|[#1704](https://github.com/Intel-bigdata/OAP/issues/1704)|[oap-native-sql] Add ColumnarUnion and ColumnarExpand|
|[#1647](https://github.com/Intel-bigdata/OAP/issues/1647)|[oap-native-sql] row to columnar for decimal|
|[#1638](https://github.com/Intel-bigdata/OAP/issues/1638)|[oap-native-sql] adding full TPC-DS support|
|[#1498](https://github.com/Intel-bigdata/OAP/issues/1498)|[oap-native-sql] stddev_samp support|
|[#1547](https://github.com/Intel-bigdata/OAP/issues/1547)|[oap-native-sql] adding metrics for input/output batches|

### Performance
|||
|:---|:---|
|[#1955](https://github.com/Intel-bigdata/OAP/issues/1955)|[OAP-CACHE] Plasma shows lower performance comparing with vanilla spark.|
|[#2023](https://github.com/Intel-bigdata/OAP/issues/2023)|[OAP-MLlib] Use oneAPI official release instead of beta versions|
|[#1829](https://github.com/Intel-bigdata/OAP/issues/1829)|[oap-native-sql] Optimize columnar shuffle and option to use AVX512|
|[#1734](https://github.com/Intel-bigdata/OAP/issues/1734)|[oap-native-sql] use non-codegen for sort with one key|
|[#1706](https://github.com/Intel-bigdata/OAP/issues/1706)|[oap-native-sql] Optimize columnar shuffle write|

### Bugs Fixed
|||
|:---|:---|
|[#2054](https://github.com/Intel-bigdata/OAP/issues/2054)|[OAP-MLlib] Faild run Intel mllib after updating the version of oneapi.|
|[#2012](https://github.com/Intel-bigdata/OAP/issues/2012)|[SQL Data Source Cache] The task will be suspended when using plasma cache.|
|[#1640](https://github.com/Intel-bigdata/OAP/issues/1640)|[SQL Data Source Cache] The task will be suspended when using plasma cache and starting 2 executors per worker.|
|[#2028](https://github.com/Intel-bigdata/OAP/issues/2028)|[OAP-Cache]When using Plasma Spark webUI OAP Tab cache metrics are not right  |
|[#1979](https://github.com/Intel-bigdata/OAP/issues/1979)|[SDLe][native-sql-engine] Issues from Static Code Analysis with Klocwork need to be fixed |
|[#1938](https://github.com/Intel-bigdata/OAP/issues/1938)|[oap-native-sql] Stability test failed when running TPCH for 10 rounds.|
|[#1924](https://github.com/Intel-bigdata/OAP/issues/1924)|[OAP-CACHE] Decouple hearbeat message and use conf to determine whether to report locailty information|
|[#1921](https://github.com/Intel-bigdata/OAP/issues/1921)|[SDLe][rpmem-shuffle] The master branch and branch-1.0-spark-3.0 can't pass BDBA analysis with libsqlitejdbc dependency.|
|[#1743](https://github.com/Intel-bigdata/OAP/issues/1743)|[oap-native-sql] Error not reported when creating CodeGenerator instance|
|[#1864](https://github.com/Intel-bigdata/OAP/issues/1864)|[oap-native-sql] hash conflict in hashagg|
|[#1934](https://github.com/Intel-bigdata/OAP/issues/1934)|[oap-native-sql] backport to 1.0|
|[#1929](https://github.com/Intel-bigdata/OAP/issues/1929)|[oap-native-sql] memleak in non-codegen aggregate|
|[#1907](https://github.com/Intel-bigdata/OAP/issues/1907)|[OAP-cache]Cannot find the class of redis-client|
|[#1742](https://github.com/Intel-bigdata/OAP/issues/1742)|[oap-native-sql] SortArraysToIndicesKernel: incorrect null ordering with multiple sort keys|
|[#1854](https://github.com/Intel-bigdata/OAP/issues/1854)|[oap-native-sql] Fix columnar shuffle file not deleted|
|[#1844](https://github.com/Intel-bigdata/OAP/issues/1844)|[oap-native-sql] Fix columnar shuffle spilled file not deleted|
|[#1580](https://github.com/Intel-bigdata/OAP/issues/1580)|[oap-native-sql] Hash Collision in multiple keys scenario|
|[#1754](https://github.com/Intel-bigdata/OAP/issues/1754)|[Intel MLlib] Improve LibLoader creating temp dir name with UUID|
|[#1825](https://github.com/Intel-bigdata/OAP/issues/1825)|Fail to run PMemBlockPlatformTest when building oap cache|
|[#1815](https://github.com/Intel-bigdata/OAP/issues/1815)|[oap-native-sql] Memory management: Error on task end if there are unclosed child allocators|
|[#1808](https://github.com/Intel-bigdata/OAP/issues/1808)|[oap-native-sql] ColumnarWindow: Memory leak on converting input/output batches|
|[#1806](https://github.com/Intel-bigdata/OAP/issues/1806)|[oap-native-sql] Fix Columnar Shuffle Memory Leak|
|[#1783](https://github.com/Intel-bigdata/OAP/issues/1783)|[oap-native-sql] ColumnarWindow: Rank() returns wrong result when input row number >= 65536|
|[#1776](https://github.com/Intel-bigdata/OAP/issues/1776)|[oap-native-sql] memory leakage in native code|
|[#1760](https://github.com/Intel-bigdata/OAP/issues/1760)|[oap-native-sql] fix columnar sorting on string|
|[#1733](https://github.com/Intel-bigdata/OAP/issues/1733)|[oap-native-sql]TPCH Q18 memory leakage |
|[#1694](https://github.com/Intel-bigdata/OAP/issues/1694)|[oap-native-sql] TPC-H q15 failed for ConditionedProbeArraysVisitorImpl MakeResultIterator does not support dependency type other than Batch|
|[#1682](https://github.com/Intel-bigdata/OAP/issues/1682)|[oap-native-sql] fix aggregate without codegen|
|[#1707](https://github.com/Intel-bigdata/OAP/issues/1707)|[oap-native-sql] Fix collect batch metric|
|[#1669](https://github.com/Intel-bigdata/OAP/issues/1669)|[oap-native-sql] TPCH Q1 results is not correct w/ hashagg codegen off|
|[#1629](https://github.com/Intel-bigdata/OAP/issues/1629)|[oap-native-sql] clean up building steps|
|[#1602](https://github.com/Intel-bigdata/OAP/issues/1602)|[oap-native-sql] rework copyfromjar function|
|[#1599](https://github.com/Intel-bigdata/OAP/issues/1599)|[oap-native-sql] Columnar BHJ fail on TPCH-Q15|
