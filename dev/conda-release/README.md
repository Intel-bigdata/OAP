# Conda recipes

This directory contains some recipes to build OAP Conda package. 

### Prerequisites for building
You need to install Conda on the build system.
```$xslt
wget -c https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
chmod 777 Miniconda2-latest-Linux-x86_64.sh 
bash Miniconda2-latest-Linux-x86_64.sh 
```
Then you should create a conda environment to build OAP Conda package.
```$xslt
conda create -n oapbuild python=3.7
conda activate oapbuild
```
Conda install packages on the build system listed below.
```$xslt
conda install anaconda-client conda-build
```
Log in anaconda.
```$xslt
anaconda login
```


### Build OAP Conda package
```$xslt
cd $OAP_HOME/dev/create-release/conda-recipes/
sh make-conda-release.sh
```