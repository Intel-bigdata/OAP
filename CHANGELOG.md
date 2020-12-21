# Change log
Generated on 2020-12-21

## Release 1.0.0

### Features
|||
|:---|:---|
|[#1823](https://github.com/Intel-bigdata/OAP/issues/1823)|[oap-native-sql][doc] Spark Native SQL Engine installation guide is obsolete and thus broken.|
|[#1545](https://github.com/Intel-bigdata/OAP/issues/1545)|[oap-data-source][arrow] Add metric: output_batches|
|[#1588](https://github.com/Intel-bigdata/OAP/issues/1588)|[OAP-CACHE] Make Parquet file splitable|
|[#1337](https://github.com/Intel-bigdata/OAP/issues/1337)|[oap-cacnhe] Discard OAP data format|
|[#1679](https://github.com/Intel-bigdata/OAP/issues/1679)|[OAP-CACHE]Remove the code related to reading and writing OAP data format|
|[#1680](https://github.com/Intel-bigdata/OAP/issues/1680)|[OAP-CACHE]Decouple spark code includes FileFormatDataWriter, FileFormatWriter and OutputWriter|
|[#1846](https://github.com/Intel-bigdata/OAP/issues/1846)|[oap-native-sql] spark sql unit test|
|[#1811](https://github.com/Intel-bigdata/OAP/issues/1811)|[OAP-cache]provide one-step starting scripts like plasma-sever redis-server|
|[#1519](https://github.com/Intel-bigdata/OAP/issues/1519)|[oap-native-sql] upgrade cmake|
|[#1873](https://github.com/Intel-bigdata/OAP/issues/1873)|[oap-native-sql] Columnar shuffle split variable length use UnsafeAppend|
|[#1835](https://github.com/Intel-bigdata/OAP/issues/1835)|[oap-native-sql] Support ColumnarBHJ to Build and Broadcast HashRelation in driver side|
|[#1848](https://github.com/Intel-bigdata/OAP/issues/1848)|[OAP-CACHE]Decouple spark code include OneApplicationResource.scala|
|[#1824](https://github.com/Intel-bigdata/OAP/issues/1824)|[OAP-CACHE]Decouple spark code includes DataSourceScanExec.scala.|
|[#1838](https://github.com/Intel-bigdata/OAP/issues/1838)|[OAP-CACHE]Decouple spark code includes VectorizedColumnReader.java, VectorizedPlainValuesReader.java, VectorizedRleValuesReader.java and OnHeapColumnVector.java|
|[#1839](https://github.com/Intel-bigdata/OAP/issues/1839)|[oap-native-sql] Add prefetch to columnar shuffle split|
|[#1756](https://github.com/Intel-bigdata/OAP/issues/1756)|[Intel MLlib] Add Kmeans "tolerance" support and test cases|
|[#1818](https://github.com/Intel-bigdata/OAP/issues/1818)|[OAP-Cache]Make Spark webUI OAP Tab more user friendly|
|[#1831](https://github.com/Intel-bigdata/OAP/issues/1831)|[oap-native-sql] ColumnarWindow: Support reusing same window spec in multiple functions|
|[#1653](https://github.com/Intel-bigdata/OAP/issues/1653)|[SQL Data Source Cache]Consistency issue on "enable" and "enabled" configuration |
|[#1765](https://github.com/Intel-bigdata/OAP/issues/1765)|[oap-native-sql] Support WSCG in nativesql|
|[#1517](https://github.com/Intel-bigdata/OAP/issues/1517)|[oap-native-sql] implement SortMergeJoin|
|[#1535](https://github.com/Intel-bigdata/OAP/issues/1535)|[oap-native-sql] Add ColumnarWindowExec|
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
|[#1956](https://github.com/Intel-bigdata/OAP/issues/1956)|[OAP-MLlib]Cannot get 5x performance benefit comparing with vanilla spark.|
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
|[#1937](https://github.com/Intel-bigdata/OAP/issues/1937)|[rpmem-shuffle] Cannot pass q64.sql of TPC-DS when enable RPmem shuffle. |
|[#1951](https://github.com/Intel-bigdata/OAP/issues/1951)|[SDLe][PMem-Shuffle]Specify Scala version above 2.12.4 in pom.xml|
|[#1921](https://github.com/Intel-bigdata/OAP/issues/1921)|[SDLe][rpmem-shuffle] The master branch and branch-1.0-spark-3.0 can't pass BDBA analysis with libsqlitejdbc dependency.|
|[#1743](https://github.com/Intel-bigdata/OAP/issues/1743)|[oap-native-sql] Error not reported when creating CodeGenerator instance|
|[#1864](https://github.com/Intel-bigdata/OAP/issues/1864)|[oap-native-sql] hash conflict in hashagg|
|[#1934](https://github.com/Intel-bigdata/OAP/issues/1934)|[oap-native-sql] backport to 1.0|
|[#1929](https://github.com/Intel-bigdata/OAP/issues/1929)|[oap-native-sql] memleak in non-codegen aggregate|
|[#1907](https://github.com/Intel-bigdata/OAP/issues/1907)|[OAP-cache]Cannot find the class of redis-client|
|[#1888](https://github.com/Intel-bigdata/OAP/issues/1888)|[oap-native-sql] Add hash collision check for all HashJoins and hashAggr|
|[#1903](https://github.com/Intel-bigdata/OAP/issues/1903)|[oap-native-sql] BHJ related UT fix|
|[#1881](https://github.com/Intel-bigdata/OAP/issues/1881)|[oap-native-sql] Fix split use avx512|
|[#1742](https://github.com/Intel-bigdata/OAP/issues/1742)|[oap-native-sql] SortArraysToIndicesKernel: incorrect null ordering with multiple sort keys|
|[#1553](https://github.com/Intel-bigdata/OAP/issues/1553)|[oap-native-sql] TPCH-Q7 fails in throughput tests|
|[#1854](https://github.com/Intel-bigdata/OAP/issues/1854)|[oap-native-sql] Fix columnar shuffle file not deleted|
|[#1844](https://github.com/Intel-bigdata/OAP/issues/1844)|[oap-native-sql] Fix columnar shuffle spilled file not deleted|
|[#1580](https://github.com/Intel-bigdata/OAP/issues/1580)|[oap-native-sql] Hash Collision in multiple keys scenario|
|[#1754](https://github.com/Intel-bigdata/OAP/issues/1754)|[Intel MLlib] Improve LibLoader creating temp dir name with UUID|
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
|[#1642](https://github.com/Intel-bigdata/OAP/issues/1642)|[oap-native-sql] Support expression key in Join|
|[#1669](https://github.com/Intel-bigdata/OAP/issues/1669)|[oap-native-sql] TPCH Q1 results is not correct w/ hashagg codegen off|
|[#1629](https://github.com/Intel-bigdata/OAP/issues/1629)|[oap-native-sql] clean up building steps|
|[#1602](https://github.com/Intel-bigdata/OAP/issues/1602)|[oap-native-sql] rework copyfromjar function|
|[#1599](https://github.com/Intel-bigdata/OAP/issues/1599)|[oap-native-sql] Columnar BHJ fail on TPCH-Q15|
|[#1567](https://github.com/Intel-bigdata/OAP/issues/1567)|[oap-native-sql] Spark thrift-server does not honor LIBARROW_DIR env|
|[#1541](https://github.com/Intel-bigdata/OAP/issues/1541)|[oap-native-sql] TreeNode children not replaced by columnar operators|

### PRs
|||
|:---|:---|
|[#2056](https://github.com/Intel-bigdata/OAP/pull/2056)|[OAP-2054][OAP-MLlib] Fix oneDAL libJavaAPI.so packaging for oneAPI 2021.1 production release|
|[#2039](https://github.com/Intel-bigdata/OAP/pull/2039)|[OAP-2023][OAP-MLlib] Switch to oneAPI 2021.1.1 official release for OAP 1.0|
|[#2043](https://github.com/Intel-bigdata/OAP/pull/2043)|[OAP-1981][OAP-CACHE][POAE7-617]fix binary cache core dump issue|
|[#2002](https://github.com/Intel-bigdata/OAP/pull/2002)|[OAP-2001][oap-native-sql]fix coding style|
|[#2035](https://github.com/Intel-bigdata/OAP/pull/2035)|[OAP-2028][OAP-cache][POAE7-635] Fix set concurrent access bug|
|[#2037](https://github.com/Intel-bigdata/OAP/pull/2037)|[OAP-1640][OAP-CACHE][POAE7-593]Fix plasma hang due to threshold|
|[#2036](https://github.com/Intel-bigdata/OAP/pull/2036)|[OAP-1955][OAP-CACHE][POAE7-660]preferLocation low hit rate fix master branch|
|[#2013](https://github.com/Intel-bigdata/OAP/pull/2013)|[OAP-CACHE][POAE7-628]port missing commits from branch 0.8/0.9 |
|[#2015](https://github.com/Intel-bigdata/OAP/pull/2015)|[OAP-2016] fix klocwork issues in oap-common/oap-spark|
|[#2022](https://github.com/Intel-bigdata/OAP/pull/2022)|[OAP-1980][rpmem-shuffle] Fix Klockwork issues for spark3.x version|
|[#2011](https://github.com/Intel-bigdata/OAP/pull/2011)|[OAP-2010][oap-native-sql] Add abs support in wscg|
|[#1996](https://github.com/Intel-bigdata/OAP/pull/1996)|[OAP-1998][oap-native-sql] Add support to do numa binding for Columnar Operations|
|[#2004](https://github.com/Intel-bigdata/OAP/pull/2004)|[OAP-2012][OAP-CACHE][POAE7-635]bug fix: plasma hang - use java thread-safe set|
|[#1988](https://github.com/Intel-bigdata/OAP/pull/1988)|[OAP-1983][oap-native-sql] Fix Q38 and Q87 when unsafeRow contains null|
|[#1976](https://github.com/Intel-bigdata/OAP/pull/1976)|[OAP-1983][oap-native-sql] Fix hashCheck performance issue|
|[#1970](https://github.com/Intel-bigdata/OAP/pull/1970)|[OAP-1947][oap-native-sql][C++] reduce sort kernel memory footprint|
|[#1961](https://github.com/Intel-bigdata/OAP/pull/1961)|[OAP-1924][OAP-CACHE]Decouple hearbeat message and use conf to determine whether to report locailty information for branch branch-1.0-spark-3.x|
|[#1982](https://github.com/Intel-bigdata/OAP/pull/1982)|[OAP-1981][OAP-CACHE][POAE7-617]Bug fix binary docache|
|[#1952](https://github.com/Intel-bigdata/OAP/pull/1952)|[OAP-1951][PMem-Shuffle][SDLe]Specify Scala version in pom.xml|
|[#1919](https://github.com/Intel-bigdata/OAP/pull/1919)|[OAP-1918][OAP-CACHE][POAE7-563]bug fix: plasma get an invalid value|
|[#1589](https://github.com/Intel-bigdata/OAP/pull/1589)|[OAP-1588][OAP-CACHE][POAE7-363] Make Parquet splitable|
|[#1954](https://github.com/Intel-bigdata/OAP/pull/1954)|[OAP-1884][OAP-dev]Small fix for arrow build in prepare_oap_env.sh.|
|[#1933](https://github.com/Intel-bigdata/OAP/pull/1933)|[OAP-1934][oap-native-sql]Backport NativeSQL code to 1.0|
|[#1889](https://github.com/Intel-bigdata/OAP/pull/1889)|[OAP-1888][oap-native-sql]Add hash collision check for all HashJoins and hashAggr|
|[#1904](https://github.com/Intel-bigdata/OAP/pull/1904)|[OAP-1903][oap-native-sql] Fix Local Mode BHJ related UT fail issue|
|[#1916](https://github.com/Intel-bigdata/OAP/pull/1916)|[OAP-1846][oap-native-sql] clean up travis test|
|[#1923](https://github.com/Intel-bigdata/OAP/pull/1923)|[OAP-1921][rpmem-shuffle] For BDBA analysis to exclude unused library|
|[#1890](https://github.com/Intel-bigdata/OAP/pull/1890)|[OAP-1846][oap-native-sql] add script for running unit test|
|[#1905](https://github.com/Intel-bigdata/OAP/pull/1905)|[OAP-1813][POAE7-555] [OAP-CACHE] package redis related dependency|
|[#1908](https://github.com/Intel-bigdata/OAP/pull/1908)|[OAP-1884][OAP-dev]Add cxx-compiler in oap conda recipes for native-sql.|
|[#1901](https://github.com/Intel-bigdata/OAP/pull/1901)|[OAP-1884][OAP-dev]Add c-compiler in oap conda recipes for native-sql.|
|[#1895](https://github.com/Intel-bigdata/OAP/pull/1895)|[OAP-1884][OAP-dev] Checkout arrow branch in case arrow in other branch|
|[#1876](https://github.com/Intel-bigdata/OAP/pull/1876)|[OAP-1875]Generating changelog automatically for new releases|
|[#1812](https://github.com/Intel-bigdata/OAP/pull/1812)|[OAP-1811][OAP-cache][POAE7-486]add sbin folder|
|[#1882](https://github.com/Intel-bigdata/OAP/pull/1882)|[OAP-1881][oap-native-sql] Fix split use avx512|
|[#1847](https://github.com/Intel-bigdata/OAP/pull/1847)|[OAP-1846][oap-native-sql] add unit tests from spark to native sql|
|[#1836](https://github.com/Intel-bigdata/OAP/pull/1836)|[OAP-1835][oap-native-sql] Support ColumnarBHJ to build and broadcast hashrelation|
|[#1885](https://github.com/Intel-bigdata/OAP/pull/1885)|[OAP-1884][OAP-dev]Add oap-mllib to parent pom and fix error when git clone oneccl.|
|[#1868](https://github.com/Intel-bigdata/OAP/pull/1868)|[OAP-1653][OAP-Cache]Modify enabled and enable compatibility check|
|[#1853](https://github.com/Intel-bigdata/OAP/pull/1853)|[OAP-1852][oap-native-sql] Memory Management: Use Arrow C++ memory po…|
|[#1859](https://github.com/Intel-bigdata/OAP/pull/1859)|[OAP-1858][OAP-cache][POAE7-518] Decouple FilePartition.scala|
|[#1857](https://github.com/Intel-bigdata/OAP/pull/1857)|[OAP-1833][oap-native-sql] Fix HashAggr hasNext won't stop issue|
|[#1855](https://github.com/Intel-bigdata/OAP/pull/1855)|[OAP-1854][oap-native-sql] Fix columnar shuffle file not deleted|
|[#1840](https://github.com/Intel-bigdata/OAP/pull/1840)|[OAP-1839][oap-native-sql] Add prefetch to columnar shuffle split|
|[#1843](https://github.com/Intel-bigdata/OAP/pull/1843)|[OAP-1842][OAP-dev]Add arrow conda build action job.|
|[#1849](https://github.com/Intel-bigdata/OAP/pull/1849)|[OAP-1848][SQL Data Source Cache] Decouple OneApplicationResource.scala|
|[#1837](https://github.com/Intel-bigdata/OAP/pull/1837)|[OAP-1838][SQL Data Source Cache] Decouple VectorizedColumnReader.java, VectorizedPlainValuesReader.java, VectorizedRleValuesReader.java and OnHeapColumnVector.java.|
|[#1757](https://github.com/Intel-bigdata/OAP/pull/1757)|[OAP-1756][Intel MLlib] Add Kmeans "tolerance" support and test cases|
|[#1845](https://github.com/Intel-bigdata/OAP/pull/1845)|[OAP-1844][oap-native-sql] Fix columnar shuffle spilled file not deleted|
|[#1827](https://github.com/Intel-bigdata/OAP/pull/1827)|[OAP-1818][SQL-Data-Source-Cache]Modify Spark webUI OAP Tab expressio…|
|[#1832](https://github.com/Intel-bigdata/OAP/pull/1832)|[OAP-1831][oap-native-sql] ColumnarWindow: Support reusing same windo…|
|[#1834](https://github.com/Intel-bigdata/OAP/pull/1834)|[OAP-1833][oap-native-sql][Scala] fix CoalesceBatchs after HashAgg|
|[#1830](https://github.com/Intel-bigdata/OAP/pull/1830)|[OAP-1829][oap-native-sql] Optimize columnar shuffle and option to use AVX-512|
|[#1803](https://github.com/Intel-bigdata/OAP/pull/1803)|[OAP-1751][oap-native-sql]fix sort on TPC-DS|
|[#1755](https://github.com/Intel-bigdata/OAP/pull/1755)|[OAP-1754][Intel MLlib] Improve LibLoader creating temp dir name with UUID|
|[#1826](https://github.com/Intel-bigdata/OAP/pull/1826)|[OAP-1825] disable pmemblk test|
|[#1802](https://github.com/Intel-bigdata/OAP/pull/1802)|[OAP-1653][OAP-Cache]Keep consistency on 'enabled' of OapConf configu…|
|[#1810](https://github.com/Intel-bigdata/OAP/pull/1810)|[OAP-1771]Fix README for Arrow Data Source|
|[#1816](https://github.com/Intel-bigdata/OAP/pull/1816)|[OAP-1815][oap-native-sql] Memory management: Error on task end if th…|
|[#1809](https://github.com/Intel-bigdata/OAP/pull/1809)|[OAP-1808][oap-native-sql] ColumnarWindow: Memory leak on converting input/output batches|
|[#1467](https://github.com/Intel-bigdata/OAP/pull/1467)|[OAP-1457][oap-native-sql] Reserve Spark off-heap execution memory after buffer allocation|
|[#1807](https://github.com/Intel-bigdata/OAP/pull/1807)|[OAP-1806][oap-native-sql] Fix Columnar Shuffle Memory Leak|
|[#1788](https://github.com/Intel-bigdata/OAP/pull/1788)|[OAP-1765][oap-native-sql] Fix for dropped CoalecseBatches before ColumnarBroadcastExchange|
|[#1799](https://github.com/Intel-bigdata/OAP/pull/1799)|[OAP-CACHE][OAP-1690][POAE7-430] Cache backend fall back detect bug fix branch master|
|[#1744](https://github.com/Intel-bigdata/OAP/pull/1744)|[OAP-CACHE][OAP-1748][POAE7-462] Enable externalDB to store CacheMetaInfo branch master|
|[#1787](https://github.com/Intel-bigdata/OAP/pull/1787)|[OAP-1786][oap-native-sql] ColumnarWindow: Avoid unnecessary mem copies|
|[#1773](https://github.com/Intel-bigdata/OAP/pull/1773)|[POAE7-471]Handle oap-common build issue about PMemKV|
|[#1782](https://github.com/Intel-bigdata/OAP/pull/1782)|[OAP-1631]Update compile scripts from 0.9|
|[#1785](https://github.com/Intel-bigdata/OAP/pull/1785)|[OAP-1765][oap-native-sql] Support WSCG for nativesql(PART 2)|
|[#1781](https://github.com/Intel-bigdata/OAP/pull/1781)|[OAP-1765][oap-native-sql] fix codegen for SMJ and HashAgg|
|[#1775](https://github.com/Intel-bigdata/OAP/pull/1775)|[OAP-1776][oap-native-sql]fix sort memleak|
|[#1766](https://github.com/Intel-bigdata/OAP/pull/1766)|[OAP-1765][oap-native-sql] Support WSCG for nativesql and use non-codegen join for remainings|
|[#1774](https://github.com/Intel-bigdata/OAP/pull/1774)|[OAP-1631]Add prepare_oap_env.sh.|
|[#1769](https://github.com/Intel-bigdata/OAP/pull/1769)|[OAP-1768][POAE7-163][OAP-SPARK] Integrate block manager with chunk api|
|[#1763](https://github.com/Intel-bigdata/OAP/pull/1763)|[OAP-1759][oap-native-sql] ColumnarWindow: Add execution metrics|
|[#1656](https://github.com/Intel-bigdata/OAP/pull/1656)|[OAP-1517][oap-native-sql] Improve SortMergeJoin Part2|
|[#1761](https://github.com/Intel-bigdata/OAP/pull/1761)|[oap-native-sql] quick fix sort on string by fallback to row|
|[#1536](https://github.com/Intel-bigdata/OAP/pull/1536)|[OAP-1535][oap-native-sql] Add ColumnarWindowExec|
|[#1735](https://github.com/Intel-bigdata/OAP/pull/1735)|[OAP-1734][oap-native-sql]use non-codegen for sort with single key|
|[#1747](https://github.com/Intel-bigdata/OAP/pull/1747)|[OAP-1741][rpmem-shuffle]To make java side load native library from jar directly|
|[#1725](https://github.com/Intel-bigdata/OAP/pull/1725)|[OAP-1727][POAE7-358] Spark integration: Memory Spill to PMem|
|[#1738](https://github.com/Intel-bigdata/OAP/pull/1738)|[OAP-1733][oap-native-sql][Scala] fix mem leak|
|[#1701](https://github.com/Intel-bigdata/OAP/pull/1701)|[OAP-1700][oap-native-sql] support join-inside condition project|
|[#1736](https://github.com/Intel-bigdata/OAP/pull/1736)|[oap-1727][POAE7-358] Add native spark files for memory spill module|
|[#1719](https://github.com/Intel-bigdata/OAP/pull/1719)|[oap-common][POAE7-347]Stream API for PMem storage store|
|[#1723](https://github.com/Intel-bigdata/OAP/pull/1723)|[OAP-1679][OAP-CACHE] Remove the code related to reading and writing OAP data format |
|[#1716](https://github.com/Intel-bigdata/OAP/pull/1716)|[OAP-1717][oap-native-sql] support null in columnar literal and subquery|
|[#1713](https://github.com/Intel-bigdata/OAP/pull/1713)|[OAP-1712] [OAP-SPARK] Remove file change list from dev directory|
|[#1711](https://github.com/Intel-bigdata/OAP/pull/1711)|[OAP-1694][oap-native-sql][Scala] fix hash join w/ empty batch|
|[#1710](https://github.com/Intel-bigdata/OAP/pull/1710)|[OAP-1706][oap-native-sql] Optimize shuffle write|
|[#1705](https://github.com/Intel-bigdata/OAP/pull/1705)|[OAP-1704][oap-native-sql] Support ColumnarUnion and ColumnarExpand|
|[#1683](https://github.com/Intel-bigdata/OAP/pull/1683)|[OAP-1682][oap-native-sql] fix aggregate without codegen|
|[#1708](https://github.com/Intel-bigdata/OAP/pull/1708)|[OAP-1707][oap-native-sql] Fix collect batch metric|
|[#1675](https://github.com/Intel-bigdata/OAP/pull/1675)|[OAP-1651][oap-native-sql] Adding fallback rules for join/shuffle|
|[#1674](https://github.com/Intel-bigdata/OAP/pull/1674)|[OAP-1673][oap-native-sql] Adding native double round function|
|[#1632](https://github.com/Intel-bigdata/OAP/pull/1632)|[OAP-1631][Doc] Add Commit Message Requirements|
|[#1672](https://github.com/Intel-bigdata/OAP/pull/1672)|[OAP-1610][Intel-MLlib]Upgrade the mahout-hdfs to version 14.1|
|[#1641](https://github.com/Intel-bigdata/OAP/pull/1641)|[OAP-1651][OAP-1642][oap-native-sql] support TPCDS w/ AQE|
|[#1670](https://github.com/Intel-bigdata/OAP/pull/1670)|[OAP-1669][oap-native-sql] use distinct ordinal list|
|[#1655](https://github.com/Intel-bigdata/OAP/pull/1655)|[OAP-1654][oap-native-sql]Columnar shuffle tpcds enabling|
|[#1630](https://github.com/Intel-bigdata/OAP/pull/1630)|[OAP-1629][oap-native-sql] clean up building scripts|
|[#1601](https://github.com/Intel-bigdata/OAP/pull/1601)|[OAP-1602][oap-native-sql][Java] fix exract resource from jar|
|[#1639](https://github.com/Intel-bigdata/OAP/pull/1639)|[OAP-1638][oap-native-sql] tpcds enabling (part2)|
|[#1586](https://github.com/Intel-bigdata/OAP/pull/1586)|[OAP-1587][oap-native-sql] tpcds enabling (part1)|
|[#1600](https://github.com/Intel-bigdata/OAP/pull/1600)|[oap-1599][oap-native-sql][Scala] fix broadcasthashjoin|
|[#1555](https://github.com/Intel-bigdata/OAP/pull/1555)|[OAP-1541][oap-native-sql] TreeNode children not replaced by columnar…|
|[#1546](https://github.com/Intel-bigdata/OAP/pull/1546)|[OAP-1547][oap-native-sql][Scala] Adding metrics for input/output batches|
|[#1472](https://github.com/Intel-bigdata/OAP/pull/1472)|[OAP-1466] [RDD Cache] [POAE-354] Initialize pmem with AppDirect and KMemDax mode in block manager |

## Release 0.8.4

### Features
|||
|:---|:---|
|[#1865](https://github.com/Intel-bigdata/OAP/issues/1865)|[OAP-CACHE]Decouple spark code include DataSourceScanExec.scala, OneApplicationResource.scala, Decouple VectorizedColumnReader.java, VectorizedPlainValuesReader.java, VectorizedRleValuesReader.java and OnHeapColumnVector.java for OAP-0.8.4.|
|[#1813](https://github.com/Intel-bigdata/OAP/issues/1813)|[OAP-cache] package redis client jar into oap-cache|

### Bugs Fixed
|||
|:---|:---|
|[#2044](https://github.com/Intel-bigdata/OAP/issues/2044)|[OAP-CACHE] Build error due to synchronizedSet on branch 0.8|
|[#2027](https://github.com/Intel-bigdata/OAP/issues/2027)|[oap-shuffle] Should load native library from jar directly|
|[#1981](https://github.com/Intel-bigdata/OAP/issues/1981)|[OAP-CACHE] Error runing q32 binary cache|
|[#1980](https://github.com/Intel-bigdata/OAP/issues/1980)|[SDLe][RPMem-Shuffle]Issues from Static Code Analysis with Klocwork need to be fixed|
|[#1918](https://github.com/Intel-bigdata/OAP/issues/1918)|[OAP-CACHE] Plasma throw exception:get an invalid value- branch 0.8|

### PRs
|||
|:---|:---|
|[#2045](https://github.com/Intel-bigdata/OAP/pull/2045)|[OAP-2044][OAP-CACHE]bug fix: build error due to synchronizedSet|
|[#2031](https://github.com/Intel-bigdata/OAP/pull/2031)|[OAP-1955][OAP-CACHE][POAE7-667]preferLocation low hit rate fix branch 0.8|
|[#2029](https://github.com/Intel-bigdata/OAP/pull/2029)|[OAP-2027][rpmem-shuffle] Load native libraries from jar|
|[#2018](https://github.com/Intel-bigdata/OAP/pull/2018)|[OAP-1980][SDLe][rpmem-shuffle] Fix potential risk issues reported by Klockwork|
|[#1920](https://github.com/Intel-bigdata/OAP/pull/1920)|[OAP-1924][OAP-CACHE]Decouple hearbeat message and use conf to determine whether to report locailty information|
|[#1949](https://github.com/Intel-bigdata/OAP/pull/1949)|[OAP-1948][rpmem-shuffle] Fix several vulnerabilities reported by BDBA|
|[#1900](https://github.com/Intel-bigdata/OAP/pull/1900)|[OAP-1680][OAP-CACHE] Decouple FileFormatDataWriter, FileFormatWriter and OutputWriter|
|[#1899](https://github.com/Intel-bigdata/OAP/pull/1899)|[OAP-1679][OAP-CACHE] Remove the code related to reading and writing OAP data format  (#1723)|
|[#1897](https://github.com/Intel-bigdata/OAP/pull/1897)|[OAP-1884][OAP-dev] Update memkind version and copy arrow plasma jar to conda package build path|
|[#1883](https://github.com/Intel-bigdata/OAP/pull/1883)|[OAP-1568][OAP-CACHE] Cleanup Oap data format read/write related test cases|
|[#1863](https://github.com/Intel-bigdata/OAP/pull/1863)|[OAP-1865][SQL Data Source Cache]Decouple spark code include DataSourceScanExec.scala, OneApplicationResource.scala, Decouple VectorizedColumnReader.java, VectorizedPlainValuesReader.java, VectorizedRleValuesReader.java and OnHeapColumnVector.java for OAP-0.8.4.|
|[#1841](https://github.com/Intel-bigdata/OAP/pull/1841)|[OAP-1579][OAP-cache]Fix web UI to show cache size|
|[#1814](https://github.com/Intel-bigdata/OAP/pull/1814)|[OAP-cache][OAP-1813][POAE7-481]package redis client related dependency|
|[#1790](https://github.com/Intel-bigdata/OAP/pull/1790)|[OAP-CACHE][OAP-1690][POAE7-430] Cache backend fallback bugfix|
|[#1740](https://github.com/Intel-bigdata/OAP/pull/1740)|[OAP-CACHE][OAP-1748][POAE7-453]Enable externalDB to store CacheMetaInfo branch 0.8|
|[#1731](https://github.com/Intel-bigdata/OAP/pull/1731)|[OAP-CACHE] [OAP-1730] [POAE-428] Add OAP cache runtime enable|
