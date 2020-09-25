# OAP Installation Guide
This document introduces how to install OAP and its dependencies on your cluster nodes by **Conda** . Certain libraries need to be compiled and installed on your system using ***root*** account. 
Follow steps below on ***every node*** of your cluster to set right environment for each machine.

## Contents
  - [Prerequisites](#prerequisites)
  - [Installing OAP](#installing-oap)
  - [Configuration](#configuration)

## Prerequisites 

- **OS Requirements**  
We have tested OAP on Fedora 29 and CentOS 7.6 (kernel-4.18.16). We recommend you use **Fedora 29 CentOS 7.6 or above**. Besides, for [Memkind](https://github.com/memkind/memkind/tree/v1.10.1-rc2) we recommend you use **kernel above 3.10**.

- **Conda Requirements**   
Install Conda on your cluster nodes with below commands and follow the prompts on the installer screens.:
```bash
wget -c https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
chmod 777 Miniconda2-latest-Linux-x86_64.sh 
bash Miniconda2-latest-Linux-x86_64.sh 
```
For changes to take effect, close and re-open your current shell.To test your installation,  run the command `conda list` in your terminal window. A list of installed packages appears if it has been installed correctly.

## Installing OAP

Dependencies below are required by OAP and all of them are included in OAP Conda package, they will be automatically installed in your cluster when you conda install OAP. Ensure you have activated environment which you create in the previous steps.

- [Memkind](https://anaconda.org/intel/memkind)
- [Vmemcache](https://anaconda.org/intel/vmemcache)
- [HPNL](https://anaconda.org/intel/hpnl)

Create a conda environment and install OAP Conda package.
```bash
conda create -n oapenv -y python=3.7
conda activate oapenv
conda install -c conda-forge -c intel -y oap=0.9.0
```
Once finished steps above, you have completed OAP dependencies installation and OAP building, and will find built OAP jars in `/root/miniconda2/envs/oapenv/oap_jars/`

***NOTE***: **Shuffle Remote PMem Extension**  
If you use one of OAP features -- [Shuffle Remote PMem Extension](../oap-shuffle/RPMem-shuffle/README.md), there are 2 points to note.
 
1. Shuffle Remote PMem Extension needs to install library [PMDK](https://github.com/pmem/pmdk) which we haven't provided a Conda package, so you can run commands below to enable PMDK.

```
git clone -b <tag-version> https://github.com/Intel-bigdata/OAP.git
cd OAP/
sh dev/install-runtime-dependencies.sh 

```
2. If you also want to use Shuffle Remote PMem Extension with **RDMA**, you need to configure and validate RDMA, please refer to [Shuffle Remote PMem Extension Guide](../oap-shuffle/RPMem-shuffle/README.md#4-configure-and-validate-rdma) for the details.


##  Configuration
Once finished steps above, make sure libraries installed by Conda can be linked by Spark, please add the following configuration settings to `$SPARK_HOME/conf/spark-defaults` on the working node.

```
spark.executorEnv.LD_LIBRARY_PATH /root/miniconda2/envs/oapenv/lib/
spark.executor.extraLibraryPath /root/miniconda2/envs/oapenv/lib/
spark.driver.extraLibraryPath /root/miniconda2/envs/oapenv/lib/
spark.executor.extraClassPath      /root/miniconda2/envs/oapenv/oap_jars/$OAP_FEATURE.jar
spark.driver.extraClassPath      /root/miniconda2/envs/oapenv/oap_jars/$OAP_FEATURE.jar
```

And then you can follow the corresponding feature documents for more details to use them.

* [OAP User Guide](../README.md#user-guide)




