# Intel MLlib

## Overview

Intel MLlib is an optimized package to accelerate machine learning algorithms in  [Apache Spark MLlib](https://spark.apache.org/mllib).  It is compatible with Spark MLlib and leverages open source [Intel® oneAPI Data Analytics Library (oneDAL)](https://github.com/oneapi-src/oneDAL) to provide highly optimized algorithms and get most out of CPU and GPU capabilities. It also take advantage of open source [Intel® oneAPI Collective Communications Library (oneCCL)](https://github.com/oneapi-src/oneCCL) to provide efficient communication patterns in multi-node multi-GPU clusters.

## Compatibility

Intel MLlib tried to maintain the same API interfaces and produce same results that are identical with Spark MLlib. However due to the nature of float point operations, there may be some small deviation from the original result, we will try our best to make sure the error is within acceptable range.
For those algorithms that are not accelerated by Intel MLlib, the original Spark MLlib one will be used. 

## Getting Started

### Java/Scala Users Preferred

Use a pre-built Intel MLlib JAR to get started. You can firstly download OAP package from [OAP-JARs-Tarball](https://github.com/Intel-bigdata/OAP/releases/download/v0.9.0-spark-3.0.0/oap-0.9.0-bin-spark-3.0.0.tar.gz) and extract this Tarball to get `oap-mllib-x.x.x-with-spark-x.x.x.jar` under `oap-0.9.0-bin-spark-3.0.0/jars`.

Then you can refer to the following [Running](#Running) section to try out.

### Python/PySpark Users Preferred

Use a pre-built JAR to get started. If you have finished [OAP-Installation-Guide](../docs/OAP-Installation-Guide.md), you can find compiled Intel MLlib JAR `oap-mllib-x.x.x-with-spark-x.x.x.jar` in `$HOME/miniconda2/envs/oapenv/oap_jars/`.

Then you can refer to the following [Running](#Running) section to try out.

### Building From Scratch

You can also build the package from source code, please refer to [Building](#Building) section.

## Running

### Prerequisites

* CentOS 7.0+, Ubuntu 18.04 LTS+
* Java JRE 8.0+ Runtime
* Apache Spark 3.0.0+

Generally, our common system requirements are the same with Intel® oneAPI Toolkit, please refer to [here](https://software.intel.com/content/www/us/en/develop/articles/intel-oneapi-base-toolkit-system-requirements.html) for details.

Intel® oneAPI Toolkits (Beta) components used by the project are already included into JAR package mentioned above. There are no extra installations for cluster nodes.

### Spark Configuration

Users usually run Spark application on __YARN__ with __client__ mode. In that case, you only need to add the following configurations in `spark-defaults.conf` or in `spark-submit` command line before running. 

```
# absolute path of the jar for uploading
spark.files                       /path/to/oap-mllib-x.x.x-with-spark-x.x.x.jar
# absolute path of the jar for driver class path
spark.driver.extraClassPath       /path/to/oap-mllib-x.x.x-with-spark-x.x.x.jar
# relative path of the jar for executor class path
spark.executor.extraClassPath     ./oap-mllib-x.x.x-with-spark-x.x.x.jar
```

### Sanity Check

To use K-means example for sanity check, you need to upload a data file to your HDFS and change related variables in `run.sh` of kmeans example. Then run the following commands:
```
    $ cd OAP/oap-mllib/examples/kmeans
    $ ./build.sh
    $ ./run.sh
```

### Benchmark with HiBench
Use [Hibench](https://github.com/Intel-bigdata/HiBench) to generate dataset with various profiles, and change related variables in `run-XXX.sh` script when applicable.  Then run the following commands:
```
    $ cd OAP/oap-mllib/examples/kmeans-hibench
    $ ./build.sh
    $ ./run-hibench-oap-mllib.sh
```

### PySpark Support

As PySpark-based applications call their Scala couterparts, they shall be supported out-of-box. An example can be found in the [Examples](#Examples) section.

## Building

### Prerequisites

We use [Apache Maven](https://maven.apache.org/) to manage and build source code.  The following tools and libraries are also needed to build Intel MLlib:

* JDK 8.0+
* Apache Maven 3.6.2+
* GNU GCC 4.8.5+
* Intel® oneAPI Toolkits (Beta) 2021.1-beta07 Components: 
    - Data Analytics Library (oneDAL)
    - Threading Building Blocks (oneTBB)
* [Open Source Intel® oneAPI Collective Communications Library (oneCCL)](https://github.com/oneapi-src/oneCCL)

Intel® oneAPI Toolkits (Beta) and its components can be downloaded and install from [here](https://software.intel.com/content/www/us/en/develop/tools/oneapi.html). Installation process for oneAPI using Package Managers (YUM (DNF), APT, and ZYPPER) is also available. Generally you only need to install oneAPI Base Toolkit for Linux with all or selected components mentioned above. Instead of using oneCCL included in Intel® oneAPI Toolkits (Beta), we prefer to build from open source oneCCL to resolve some bugs.

More details abount oneAPI can be found [here](https://software.intel.com/content/www/us/en/develop/tools/oneapi.html).

__Note: We have verified the building process based on oneAPI 2021.1-beta07. Due to default installation path change in 2021.1-beta08+, it will not work for 2021.1-beta08+. We will fix it soon. You can also refer to [this script and comments in it](https://github.com/Intel-bigdata/OAP/blob/master/oap-mllib/dev/install-build-deps-centos.sh) to install correct oneAPI version and manually setup the environments.__

Scala and Java dependency descriptions are already included in Maven POM file. 

### Build

####  Building oneCCL

To clone and build from open source oneCCL, run the following commands:
```
	$ git clone https://github.com/oneapi-src/oneCCL
        $ cd oneCCL
        $ git checkout -b 2021.1-beta07-1 origin/2021.1-beta07-1
	$ mkdir build && cd build
	$ cmake ..
	$ make -j install
```

The generated files will be placed in `/your/oneCCL_source_code/build/_install`

#### Building Intel MLlib

To clone and checkout source code, run the following commands:
```
    $ git clone https://github.com/Intel-bigdata/OAP    
```
__Optional__ to checkout specific release branch:
```
    $ git checkout -b branch-0.9-spark-3.x origin/branch-0.9-spark-3.x
```

We rely on environment variables to find required toolchains and libraries. Please make sure the following environment variables are set for building:

Environment | Description
------------| -----------
JAVA_HOME   | Path to JDK home directory
DAALROOT    | Path to oneDAL home directory
TBB_ROOT    | Path to oneTBB home directory
CCL_ROOT    | Path to oneCCL home directory

We suggest you to source `setvars.sh` script into current shell to setup building environments as following:

```
	$ source /opt/intel/inteloneapi/setvars.sh
	$ source /your/oneCCL_source_code/build/_install/env/setvars.sh
```

__Be noticed we are using our own built oneCCL instead, we should source oneCCL's `setvars.sh` to overwrite oneAPI one.__

If you prefer to buid your own open source [oneDAL](https://github.com/oneapi-src/oneDAL), [oneTBB](https://github.com/oneapi-src/oneTBB) versions rather than use the ones included in oneAPI TookKits, you can refer to the related build instructions and manually source `setvars.sh` accordingly.

To build, run the following commands: 
```
    $ cd OAP/oap-mllib/mllib-dal
    $ ./build.sh
```

The built JAR package will be placed in `target` directory with the name `oap-mllib-x.x.x-with-spark-x.x.x.jar`.

## Examples

Example         |  Description 
----------------|---------------------------
kmeans          |  K-means example for Scala
kmeans-pyspark  |  K-means example for PySpark
kmeans-hibench  |  Use HiBench-generated input dataset to benchmark K-means performance

## List of Accelerated Algorithms

* K-Means (CPU, Experimental)
