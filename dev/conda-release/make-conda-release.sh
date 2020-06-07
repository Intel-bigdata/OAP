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
  memkind_repo="https://github.com/Intel-bigdata/memkind.git"
  cd $DEV_PATH/thirdparty
  if [ ! -d "memkind" ]; then
    git clone $memkind_repo
  fi
  cd memkind/
  git checkout v1.10.0-oap-0.7
  cp $RECIPES_PATH/memkind/configure.ac $DEV_PATH/thirdparty/memkind/

  cd $RECIPES_PATH/memkind/
  conda build .
}


function conda_build_vmemcache() {
  cd $DEV_PATH/thirdparty
  vmemcache_repo="https://github.com/pmem/vmemcache.git"
  if [ ! -d "vmemcache" ]; then
    git clone vmemcache_repo
  fi
  cd vmemcache/
  git pull

  cd $RECIPES_PATH/vmemcache/
  conda build .
}

function conda_build_arrow() {
  cd $DEV_PATH/thirdparty
  arrow_repo=" https://github.com/Intel-bigdata/arrow.git"
  if [ ! -d "arrow" ]; then
    git clone arrow_repo
  fi
  cd arrow
  git pull
  cp $DEV_PATH/thirdparty/arrow/dev/tasks/conda-recipes/.ci_support/linux_python3.7.yaml $DEV_PATH/thirdparty/arrow/dev/tasks/conda-recipes/arrow-cpp/
  cd $DEV_PATH/thirdparty/arrow/dev/tasks/conda-recipes/arrow-cpp
  mv linux_python3.7.yaml conda_build_config.yaml
  cp $RECIPES_PATH/intel-arrow/build.sh $DEV_PATH/thirdparty/arrow/dev/tasks/conda-recipes/arrow-cpp/
  echo "ARROW_VERSION:" >> conda_build_config.yaml
  echo "- 0.17.0" >> conda_build_config.yaml
  conda build .
}

function conda_build_oap_arrow_lib() {
  cp -L $DEV_PATH/thirdparty/arrow/cpp/release-build/release/libarrow.so cp $RECIPES_PATH/oap-arrow-lib/
  cp -L $DEV_PATH/thirdparty/arrow/cpp/release-build/release/libgandiva.so $RECIPES_PATH/oap-arrow-lib/
  cp -L $DEV_PATH/thirdparty/arrow/cpp/release-build/release/libplasma.so $RECIPES_PATH/oap-arrow-lib/
  cp -L $DEV_PATH/thirdparty/arrow/cpp/release-build/release/libplasma_java.so $RECIPES_PATH/oap-arrow-lib/
  cp $DEV_PATH/thirdparty/arrow/cpp/release-build/release/plasma-store-server $RECIPES_PATH/oap-arrow-lib/
  cd $RECIPES_PATH/oap-arrow-lib/
  conda build .
}

function conda_build_oap() {
  OAP_VERSION=0.9.0
  SPARK_VERSION=2.4.4
  sh $DEV_PATH/make-distribution.sh
  cd $DEV_PATH/release-package/
  tar -xzvf oap-$version.tar.gz
  cp $DEV_PATH/release-package/oap-product-$OAP_VERSION-bin-spark-$SPARK_VERSION/jars/* $RECIPES_PATH/oap/
  cd $RECIPES_PATH/oap/
  conda build .

}

function conda_build_all() {
  conda_build_memkind
  conda_build_vmemcache
  conda_build_arrow
  conda_build_oap
}


