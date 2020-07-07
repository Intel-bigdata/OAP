# Intel MLlib

## Overview

Intel MLlib is an optimized package to accelerate machine learning algorithms in  [Apache Spark MLlib](https://spark.apache.org/mllib).  It is compatible with Spark MLlib and leverages open source [Intel® oneAPI Data Analytics Library (oneDAL)](https://github.com/oneapi-src/oneDAL)  to provide highly optimized algorithms and get most out of CPU and GPU capabilities. It also take advantage of open source [Intel® oneAPI Collective Communications Library (oneCCL)](https://github.com/oneapi-src/oneCCL) to provide efficient communication patterns in multi-node multi-GPU clusters.

## Compatibility

Intel MLlib tried to maintain the same API interfaces and produce same results that are identical with Spark MLlib. However due to the nature of float point operations, there may be some small deviation from the original result, we will try our best to make sure the error is within acceptable range.
For those algorithms that are not accelerated by Intel MLlib, the original Spark MLlib one will be used. 

## Getting Started

You can use a pre-built package to get started, it can be downloaded from [here](https://github.com/Intel-bigdata/OAP/releases/download/v0.9.0-spark-3.0.0/oap-mllib-0.9.0-with-spark-3.0.0.jar).

After downloaded, you can refer to the following [Running](#Running) section to try out.

You can also build the package from source code, please refer to [Building](#Building) section.

## Running

### Prerequisites

* CentOS 7.0+, Ubuntu 18.04 LTS+
* Java 8.0+
* Apache Spark 3.0.0+
* Intel® oneAPI Toolkits (Beta) 2021.1-beta07+ Components: 
    - Data Analytics Library (oneDAL)
    - Threading Building Blocks (oneTBB)
    - Collective Communications Library (oneCCL)

Generally, our common system requirements are the same with Intel® oneAPI Toolkit, please refer to [here](https://software.intel.com/content/www/us/en/develop/articles/intel-oneapi-base-toolkit-system-requirements.html) for details.

Intel® oneAPI Toolkits (Beta) and its components can be downloaded and install from [here](https://software.intel.com/content/www/us/en/develop/tools/oneapi.html). Installation process for oneAPI using Package Managers (YUM (DNF), APT, and ZYPPER) is also available from [here](https://software.intel.com/content/www/us/en/develop/articles/oneapi-repo-instructions.html). Generally you only need to install __oneAPI Base Toolkit for Linux__ with all or selected components. 

We suggest you to add oneAPI Toolkits' `setvars.sh` script to shell startup script in __all cluster nodes__ so that the oneAPI library dependencies will be automatically resolved. Add the following line in `~/.bashrc`:
```
source /opt/intel/inteloneapi/setvars.sh &> /dev/null
```

### Spark Configuration

Except oneAPI components, all other dependencies are packaged into jar, you only need to set extra class path for Spark to point to this jar and `spark-submit` script will take care of the rest. 
```
spark.driver.extraClassPath=/path/to/oap-mllib-jar
spark.executor.extraClassPath=/path/to/oap-mllib-jar
```

### Sanity Check

To use K-means example for sanity check, you need to upload a data file to your HDFS and change related variables in `run.sh` of kmeans example. Then run the following commands:
```
    $ cd OAP/oap-mllib/examples/kmeans
    $ ./build.sh
    $ ./run.sh
```

### Benchmark with HiBench
Use HiBench to generate dataset with various profiles, and change related variables in `run-XXX.sh` script when applicable.  Then run the following commands:
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
* Intel® oneAPI Toolkits (Beta) 2021.1-beta07+ Components: 
    - Data Analytics Library (oneDAL)
    - Threading Building Blocks (oneTBB)
    - Collective Communications Library (oneCCL)

Please refer to the [Running](#Running) section for how to install oneAPI Toolkits. More details abount oneAPI can be found [here](https://software.intel.com/content/www/us/en/develop/tools/oneapi.html).

Scala and Java dependency descriptions are already included in Maven POM file. 

### Build

To clone and checkout source code, run the following commands:
```
    $ git clone https://github.com/Intel-bigdata/OAP
    $ git checkout -b branch-intelmllib-spark-3.0.0 origin/branch-intelmllib-spark-3.0.0
```

We rely on `JAVA_HOME` and oneAPI environments to find required toolchains and libraries. 
After installed the above Prerequisites, please make sure the following environment variables are set for building:

Environment | Description
------------| -----------
JAVA_HOME   | Path to JDK home directory
DAALROOT    | Path to oneDAL home directory
TBB_ROOT    | Path to oneTBB home directory
CCL_ROOT    | Path to oneCCL home directory

`DAALROOT`, `TBB_ROOT`, `CCL_ROOT` can be set by oneAPI Toolkits with `/opt/intel/inteloneapi/setvars.sh` script.  

If you prefer to buid your own open source [oneDAL](https://github.com/oneapi-src/oneDAL), [oneTBB](https://github.com/oneapi-src/oneTBB) or [oneCCL](https://github.com/oneapi-src/oneCCL) versions rather than use the ones included in oneAPI TookKits, you can refer to the related build instructions and manually set the paths accordingly.

To build, run the following commands: 
```
    $ cd OAP/oap-mllib
    $ ./build.sh
```

The built jar package will be placed in `target` directory with the name `oap-mllib-x.x.x-with-spark-x.x.x.jar`.

## Examples

Example         |  Description 
----------------|---------------------------
kmeans          |  K-means example for Scala
kmeans-pyspark  |  K-means example for PySpark
kmeans-hibench  |  Use HiBench-generated input dataset to benchmark K-means performance

## List of Accelerated Algorithms

* K-Means (CPU, Experimental)

