#!/bin/bash

# set -e
MAVEN_TARGET_VERSION=3.6.3

CMAKE_TARGET_VERSION=3.7.1
CMAKE_MIN_VERSION=3.3
TARGET_CMAKE_SOURCE_URL=https://cmake.org/files/v3.7/cmake-3.7.1.tar.gz

if [ -z "$DEV_PATH" ]; then
  cd $(dirname $BASH_SOURCE)
  DEV_PATH=`echo $(pwd)`
  echo $DEV_PATH
  cd -
fi

function version_lt() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" != "$1"; }

function version_ge() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$1"; }

function prepare_maven() {
  echo "Check maven version......"
  CURRENT_MAVEN_VERSION_STR="$(mvn --version)"
  if [[ "$CURRENT_MAVEN_VERSION_STR" == "Apache Maven"* ]]; then
    echo "mvn is installed"
  else
    echo "mvn is not installed"
    wget https://mirrors.cnnic.cn/apache/maven/maven-3/$MAVEN_TARGET_VERSION/binaries/apache-maven-$MAVEN_TARGET_VERSION-bin.tar.gz
    mkdir -p /usr/local/maven
    tar -xzvf apache-maven-$MAVEN_TARGET_VERSION-bin.tar.gz
    mv apache-maven-$MAVEN_TARGET_VERSION/* /usr/local/maven
    echo 'export MAVEN_HOME=/usr/local/maven' >> ~/.bashrc
    echo 'export PATH=$MAVEN_HOME/bin:$PATH' >> ~/.bashrc
    source ~/.bashrc
    rm -rf apache-maven*
  fi
}

function prepare_cmake() {
  CURRENT_CMAKE_VERSION_STR="$(cmake --version)"
  cd  $DEV_PATH

  # echo ${CURRENT_CMAKE_VERSION_STR}
  if [[ "$CURRENT_CMAKE_VERSION_STR" == "cmake version"* ]]; then
    echo "cmake is installed"
    array=(${CURRENT_CMAKE_VERSION_STR//,/ })
    CURRENT_CMAKE_VERSION=${array[2]}
    if version_lt $CURRENT_CMAKE_VERSION $CMAKE_MIN_VERSION; then
      echo "$CURRENT_CMAKE_VERSION is less than $CMAKE_MIN_VERSION,install cmake $CMAKE_TARGET_VERSION"
      mkdir -p $DEV_PATH/thirdparty
      cd $DEV_PATH/thirdparty
      echo " $DEV_PATH/thirdparty/cmake-$CMAKE_TARGET_VERSION.tar.gz"
      if [ ! -f " $DEV_PATH/thirdparty/cmake-$CMAKE_TARGET_VERSION.tar.gz" ]; then
        wget $TARGET_CMAKE_SOURCE_URL
      fi
      tar xvf cmake-$CMAKE_TARGET_VERSION.tar.gz
      cd cmake-$CMAKE_TARGET_VERSION/
      ./bootstrap
      gmake
      gmake install
      yum remove cmake -y
      ln -s /usr/local/bin/cmake /usr/bin/
      cd  $DEV_PATH
    fi
  else
    echo "cmake is not installed"
    mkdir -p $DEV_PATH/thirdparty
    cd $DEV_PATH/thirdparty
    echo " $DEV_PATH/thirdparty/cmake-$CMAKE_TARGET_VERSION.tar.gz"
    if [ ! -f "cmake-$CMAKE_TARGET_VERSION.tar.gz" ]; then
      wget $TARGET_CMAKE_SOURCE_URL
    fi

    tar xvf cmake-$CMAKE_TARGET_VERSION.tar.gz
    cd cmake-$CMAKE_TARGET_VERSION/
    ./bootstrap
    gmake
    gmake install
    cd  $DEV_PATH
  fi
}

function prepare_memkind() {
  memkind_repo="https://github.com/Intel-bigdata/memkind.git"
  echo $memkind_repo

  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  if [ ! -d "memkind" ]; then
    git clone $memkind_repo
  fi
  cd memkind/
  git pull
  git checkout v1.10.0-oap-0.7

  yum -y install autoconf
  yum -y install automake
  yum -y install gcc-c++
  yum -y install libtool
  yum -y install numactl-devel
  yum -y install unzip
  yum -y install libnuma-devel

  ./autogen.sh
  ./configure
  make
  make install
  cd  $DEV_PATH

}

function prepare_vmemcache() {
   if [ -n "$(rpm -qa | grep libvmemcache)" ]; then
    echo "libvmemcache is installed"
    return
  fi
  vmemcache_repo="https://github.com/pmem/vmemcache.git"
  prepare_cmake
  cd  $DEV_PATH
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  if [ ! -d "vmemcache" ]; then
    git clone $vmemcache_repo
  fi
  cd vmemcache
  git pull
  mkdir -p build
  cd build
  yum -y install rpm-build
  cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DCPACK_GENERATOR=rpm
  make package
  sudo rpm -i libvmemcache*.rpm
}


function  prepare_all() {
  prepare_maven
  prepare_memkind
  prepare_cmake
  prepare_vmemcache
}

function oap_build_help() {
    echo " prepare_maven      = function to install Maven"
    echo " prepare_memkind    = function to install Memkind"
    echo " prepare_cmake      = function to install Cmake"
    echo " prepare_vmemcache  = function to install Vmemcache"
    echo " prepare_all        = function to install all the above"
}