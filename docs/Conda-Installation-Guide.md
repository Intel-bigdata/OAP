# OAP Installation Guide
This document tells you how to install OAP and its dependencies on your cluster nodes. For some steps, specific libraries need to be compiled and installed to your system, the course of which requires root access. 

## Contents
  - [Prerequisites](#prerequisites)
      - [Install prerequisites](#install-prerequisites)
  - [Compiling OAP](#compiling-oap)
  - [Configuration](#configuration)

## Prerequisites 

- **OS Requirements**  
We have tested OAP on Fedora 29 and CentOS 7.6. We recommend you to use Fedora 29 and CentOS 7.6 or above.

- **Conda Requirements**   
You need to install Conda on your cluster node.
```bash
wget -c https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
chmod 777 Miniconda2-latest-Linux-x86_64.sh 
bash Miniconda2-latest-Linux-x86_64.sh 
```
    Then you should create a conda environment to install OAP Conda package.
```bash
conda create -n oapenv python=3.7
conda activate oapenv
conda config --add channels conda-forge
```

- **Requirements for Shuffle Remote PMem Extension**  
If you want to use Shuffle Remote PMem Extension with RDMA, you need to configure and validate RDMA before these installation steps. Shuffle Remote PMem Extension also need to install library [PMDK](https://github.com/pmem/pmdk) which we haven't provided a Conda package. You can refer to [Shuffle Remote PMem Extension Guide](../oap-shuffle/RPMem-shuffle/README.md#4-configure-and-validate-rdma) for the details of configuring, validating RDMA and install PMDK. 

- **Requirements for OAP MLlib**  
To enable OAP MLlib, you need to install oneDAL and oneCLL, they can be downloaded and install from [here](https://software.intel.com/content/www/us/en/develop/tools/oneapi.html). 

We provide a shell script named `prepare_oap_env.sh` to help you to completely compile and install these dependencies if your system is Fedora 29 or CentOS 7.6 or above.

```
cd OAP/
export ENABLE_RDMA=true
source ./dev/prepare_oap_env.sh
prepare_PMDK
prepare_oneAPI
```

###  Install prerequisites
Some dependencies required by OAP listed below. to use OAP, you must conda install these libraries on all nodes of your cluster. Ensure you have  activated environment which you create in the previous step.
- [Memkind](https://anaconda.org/intel-bigdata/memkind)
- [Vmemcache](https://anaconda.org/intel-bigdata/vmemcache)
- [HPNL](https://anaconda.org/intel-bigdata//hpnl)
- [Arrow-cpp](https://anaconda.org/intel-bigdata/arrow-cpp)  
- [OAP](https://anaconda.org/intel-bigdata/oap)

```bash
conda activate oapenv
conda install -c intel -c conda-forge oap=0.9
```



##  Configuration
After installing prerequisites of OAP successfully, to make sure libraries installed by Conda can be linked by Spark, please add the following configuration settings to "$SPARK_HOME/conf/spark-defaults" on the working node.

```
spark.executorEnv.LD_LIBRARY_PATH /root/miniconda2/envs/oapenv/lib/
spark.executor.extraLibraryPath /root/miniconda2/envs/oapenv/lib/
spark.driver.extraLibraryPath /root/miniconda2/envs/oapenv/lib/
spark.executor.extraClassPath      /root/miniconda2/envs/oapenv/oap_jars/$OAP_FEATURE.jar
spark.driver.extraClassPath      /root/miniconda2/envs/oapenv/oap_jars/$OAP_FEATURE.jar
```

And then you can follow the corresponding feature documents for using them.

* [OAP User Guide](../README.md#user-guide)




