#!/bin/bash

# set -e

if [ -z "$RECIPES_PATH" ]; then
  cd $(dirname $BASH_SOURCE)
  CONDA_RELEASE_PATH=`echo $(pwd)`
  echo `pwd`
  cd $CONDA_RELEASE_PATH/conda-recipes
  RECIPES_PATH=`echo $(pwd)`
  cd ../..
  DEV_PATH=`echo $(pwd)`
  echo "DEV_PATH: "$DEV_PATH
  echo "RECIPES_PATH: "$RECIPES_PATH
  cd $DEV_PATH
fi

function conda_build_memkind() {
  memkind_repo="https://github.com/memkind/memkind.git"
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  if [ ! -d "memkind" ]; then
    git clone $memkind_repo
  fi
  cd memkind/
  git checkout v1.10.1-rc2
  cp $RECIPES_PATH/memkind/configure.ac $DEV_PATH/thirdparty/memkind/
  cd $RECIPES_PATH/memkind/
  conda build .
}


function conda_build_vmemcache() {
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  vmemcache_repo="https://github.com/pmem/vmemcache.git"
  if [ ! -d "vmemcache" ]; then
    git clone $vmemcache_repo
  fi
  cd vmemcache/
  git pull
  cd $RECIPES_PATH/vmemcache/
  conda build .
}



function conda_build_libfabric() {
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  if [ ! -d "libfabric" ]; then
    git clone https://github.com/ofiwg/libfabric.git
  fi
  cd libfabric
  git checkout v1.6.0
  cd $RECIPES_PATH/libfabric
  conda build .
}

function conda_build_hpnl() {
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  if [ ! -d "HPNL" ]; then
    git clone https://github.com/Intel-bigdata/HPNL.git
  fi
  cd HPNL
  git checkout origin/spark-pmof-test --track
  git submodule update --init --recursive
  cd $RECIPES_PATH/HPNL/
  cp CMakeLists.txt $DEV_PATH/thirdparty/HPNL/
  conda build .
}

function prepare_libfabric() {
  mkdir -p $DEV_PATH/thirdparty
  yum install -y libibverbs-devel  librdmacm-devel
  cd $DEV_PATH/thirdparty
  if [ ! -d "libfabric" ]; then
    git clone https://github.com/ofiwg/libfabric.git
  fi
  cd libfabric
  git checkout v1.6.0
  ./autogen.sh
  mkdir -p $DEV_PATH/thirdparty/libfabric/release
  ./configure --disable-sockets --enable-verbs --disable-mlx --prefix=$DEV_PATH/thirdparty/libfabric/release
  make -j &&  make install
  cp $DEV_PATH/thirdparty/libfabric/release/lib/* $DEV_PATH/thirdparty/arrow/oap/libfabric
}

function conda_build_oap() {
  OAP_VERSION=0.9.0
  SPARK_VERSION=3.0.0
  sh $DEV_PATH/make-distribution.sh
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  arrow_repo="https://github.com/Intel-bigdata/arrow.git"
  if [ ! -d "arrow" ]; then
    git clone $arrow_repo -b branch-0.17.0-oap-0.9
  fi
  cd arrow
  git pull

  mkdir -p $DEV_PATH/thirdparty/arrow/oap
  cp $DEV_PATH/release-package/oap-$OAP_VERSION-bin-spark-$SPARK_VERSION/jars/* $DEV_PATH/thirdparty/arrow/oap

  cd $RECIPES_PATH/oap/
  conda build .
}

function conda_build_all() {
  conda_build_memkind
  conda_build_vmemcache
  conda_build_hpnl
  conda_build_oap
}

conda_build_all
