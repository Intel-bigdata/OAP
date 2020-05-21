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
    echo 'export MAVEN_HOME=/usr/local/maven' >>env.sh
    echo 'export PATH=$MAVEN_HOME/bin:$PATH' >>env.sh
    echo "Please source env.sh or copy it's contents to /etc/profile and source /etc/profile!"
    export MAVEN_HOME=/usr/local/maven
    export PATH=$MAVEN_HOME/bin:$PATH
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
  memkind_repo="https://github.com/memkind/memkind.git"
  echo $memkind_repo

  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  if [ ! -d "memkind" ]; then
    git clone $memkind_repo
  fi
  cd memkind/
  git pull

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

function gather() {
  cd  $DEV_PATH
  mkdir -p target
  cp ../oap-cache/oap/target/*.jar $DEV_PATH/target/
  cp ../oap-shuffle/remote-shuffle/target/*.jar $DEV_PATH/target/
  cp ../oap-common/target/*.jar $DEV_PATH/target/
  find $DEV_PATH/target -name "*test*"|xargs rm -rf
  echo "Please check the result in  $DEV_PATH/target !"
}




function prepare_intel_arrow() {
  yum -y install libpthread-stubs0-dev
  yum -y install libnuma-dev

  #install vemecache
  prepare_vmemcache

  #install arrow and plasms
  cd  $DEV_PATH/thirdparty
  if [ ! -d "arrow" ]; then
    git clone https://github.com/Intel-bigdata/arrow.git -b oap-master
  fi
  cd arrow
  git pull
  cd cpp
  rm -rf release
  mkdir -p release
  cd release

  #build libarrow, libplasma, libplasma_java
  cmake -DCMAKE_INSTALL_PREFIX=/usr/ -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS="-g -O3" -DCMAKE_CXX_FLAGS="-g -O3" -DARROW_BUILD_TESTS=on -DARROW_PLASMA_JAVA_CLIENT=on -DARROW_PLASMA=on -DARROW_DEPENDENCY_SOURCE=BUNDLED ..
  make -j$(nproc)
  make install -j$(nproc)
  cd  $DEV_PATH/thirdparty/arrow/java
  mvn clean -pl plasma -am -q -DskipTests install
}

function  prepare_all() {
  prepare_maven
  prepare_memkind
  prepare_cmake
  prepare_vmemcache
  prepare_intel_arrow
}