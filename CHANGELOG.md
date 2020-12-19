# Change log
Generated on 2020-12-19

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
|[#1828](https://github.com/Intel-bigdata/OAP/issues/1828)|OAP PmofShuffleManager log info error|
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
