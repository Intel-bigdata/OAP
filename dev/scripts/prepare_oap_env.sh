#!/bin/bash

# set -e
MAVEN_TARGET_VERSION=3.6.3
MAVEN_MIN_VERSION=3.3
CMAKE_TARGET_VERSION=3.11.1
CMAKE_MIN_VERSION=3.11
TARGET_CMAKE_SOURCE_URL=https://cmake.org/files/v3.11/cmake-3.11.1.tar.gz
GCC_MIN_VERSION=7.0
LLVM_MIN_VERSION=7.0
rx='^([0-9]+\.){0,2}(\*|[0-9]+)$'

if [ -z "$DEV_PATH" ]; then
  OAP_HOME="$(cd "`dirname "$0"`/../.."; pwd)"
  DEV_PATH=$OAP_HOME/dev/
fi

function check_jdk() {
  if [ -z "$JAVA_HOME" ]; then
    echo "To run this script, you must make sure that Java is installed in your environment and set JAVA_HOME"
    exit 1
  fi
}

function version_lt() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" != "$1"; }

function version_ge() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$1"; }

function check_gcc() {
  CURRENT_GCC_VERSION_STR="$(gcc --version)"
  array=(${CURRENT_GCC_VERSION_STR//,/ })
  CURRENT_GCC_VERSION=${array[2]}
  if version_lt $CURRENT_GCC_VERSION $GCC_MIN_VERSION; then
    if [ ! -f "$DEV_PATH/thirdparty/gcc7/bin/gcc" ]; then
      install_gcc7
    fi
    export CXX=$DEV_PATH/thirdparty/gcc7/bin/g++
    export CC=$DEV_PATH/thirdparty/gcc7/bin/gcc
    export LD_LIBRARY_PATH=$DEV_PATH/thirdparty/gcc7/lib64:$LD_LIBRARY_PATH
  fi
}

function check_maven() {
  CURRENT_MAVEN_VERSION_STR="$(mvn --version)"
  array=(${CURRENT_MAVEN_VERSION_STR//,/ })
  CURRENT_MAVEN_VERSION=${array[2]}
  echo $CURRENT_MAVEN_VERSION
  if version_lt $CURRENT_MAVEN_VERSION $MAVEN_MIN_VERSION; then
    install_maven
  fi
}

function install_maven() {
  yum -y install wget
  cd $DEV_PATH/thirdparty
  if [ ! -f " $DEV_PATH/thirdparty/apache-maven-$MAVEN_TARGET_VERSION-bin.tar.gz" ]; then
        wget --no-check-certificate https://mirrors.cnnic.cn/apache/maven/maven-3/$MAVEN_TARGET_VERSION/binaries/apache-maven-$MAVEN_TARGET_VERSION-bin.tar.gz
  fi

  cd /usr/local/
  if [ ! -d "maven" ]; then
    rm -rf /usr/local/maven
    mkdir -p /usr/local/maven
    INSTALL_PATH=/usr/local/maven
  else
    rm -rf /usr/local/maven_oap
    mkdir -p /usr/local/maven_oap
    INSTALL_PATH=/usr/local/maven_oap
  fi
  cd $DEV_PATH/thirdparty
  tar -xzvf apache-maven-$MAVEN_TARGET_VERSION-bin.tar.gz
  mv apache-maven-$MAVEN_TARGET_VERSION/* $INSTALL_PATH
  echo 'export MAVEN_HOME='$INSTALL_PATH >> ~/.bashrc
  echo 'export PATH=$MAVEN_HOME/bin:$PATH' >> ~/.bashrc
  source ~/.bashrc
  rm -rf apache-maven*
  cd $DEV_PATH/thirdparty
}

function prepare_maven() {
  echo "Check maven version......"
  CURRENT_MAVEN_VERSION_STR="$(mvn --version)"
  if [[ "$CURRENT_MAVEN_VERSION_STR" == "Apache Maven"* ]]; then
    echo "mvn is installed"
    array=(${CURRENT_MAVEN_VERSION_STR//,/ })
    CURRENT_MAVEN_VERSION=${array[2]}
    if version_lt $CURRENT_MAVEN_VERSION $MAVEN_MIN_VERSION; then
      install_maven
    fi
  else
    echo "mvn is not installed"
    install_maven
  fi
}

function prepare_cmake() {
  CURRENT_CMAKE_VERSION_STR="$(cmake --version)"
  cd  $DEV_PATH

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
        wget --no-check-certificate $TARGET_CMAKE_SOURCE_URL
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
      wget --no-check-certificate $TARGET_CMAKE_SOURCE_URL
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
  git checkout v1.10.1-rc2

  yum -y install autoconf
  yum -y install automake
  yum -y install gcc-c++
  yum -y install libtool
  yum -y install numactl-devel
  yum -y install unzip
  yum -y install make

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
  rpm -i libvmemcache*.rpm
}

function install_gcc7() {
  #for gcc7
  yum -y install gmp-devel
  yum -y install mpfr-devel
  yum -y install libmpc-devel
  yum -y install wget

  cd $DEV_PATH/thirdparty

  if [ ! -d "gcc-7.3.0" ]; then
    if [ ! -f "gcc-7.3.0.tar" ]; then
      if [ ! -f "gcc-7.3.0.tar.xz" ]; then
        wget  --no-check-certificate https://bigsearcher.com/mirrors/gcc/releases/gcc-7.3.0/gcc-7.3.0.tar.xz
      fi
      xz -d gcc-7.3.0.tar.xz
    fi
    tar -xvf gcc-7.3.0.tar
  fi

  cd gcc-7.3.0/
  mkdir -p $DEV_PATH/thirdparty/gcc7
  ./configure --prefix=$DEV_PATH/thirdparty/gcc7 --disable-multilib
  make -j
  make install
}

function prepare_llvm() {
  CURRENT_LLVM_VERSION_STR="$(llvm-config --version)"
  if [[ "CURRENT_LLVM_VERSION_STR" =~ $rx  ]]; then
    if version_ge $CURRENT_LLVM_VERSION_STR $LLVM_MIN_VERSION; then
      echo "llvm is installed"
      return
    fi
  fi
  echo "Start to build and install llvm7....."
  cd $DEV_PATH
  mkdir -p $DEV_PATH/thirdparty/llvm
  cd $DEV_PATH/thirdparty/llvm
  if [ ! -d "llvm-7.0.1.src" ]; then
    if [ ! -f "llvm-7.0.1.src.tar.xz" ]; then
      wget --no-check-certificate http://releases.llvm.org/7.0.1/llvm-7.0.1.src.tar.xz
    fi
    tar xf llvm-7.0.1.src.tar.xz
  fi
  cd llvm-7.0.1.src/
  mkdir -p tools
  cd tools

  if [ ! -d "clang" ]; then
    if [ ! -f "cfe-7.0.1.src.tar.xz" ]; then
      wget --no-check-certificate http://releases.llvm.org/7.0.1/cfe-7.0.1.src.tar.xz
      tar xf cfe-7.0.1.src.tar.xz
    fi
    mv cfe-7.0.1.src clang
  fi
  cd ..
  mkdir -p build
  cd build

  check_gcc

  cmake -DCMAKE_BUILD_TYPE=Release ..
  cmake --build .
  cmake --build . --target install

}

function prepare_intel_arrow() {
  prepare_cmake
  prepare_llvm
  yum -y install libgsasl
  yum -y install libidn-devel.x86_64
  yum -y install libntlm.x86_64
  cd $DEV_PATH
  mkdir -p $DEV_PATH/thirdparty/
  cd $DEV_PATH/thirdparty/
  intel_arrow_repo="https://github.com/Intel-bigdata/arrow.git"
  if [ ! -d "arrow" ]; then
    git clone $intel_arrow_repo -b branch-0.17.0-oap-0.9
    cd arrow
  else
    cd arrow
    git pull
  fi
  current_arrow_path=$(pwd)
  mkdir -p cpp/release-build
  check_gcc

  cd cpp/release-build
  cmake -DCMAKE_BUILD_TYPE=Release -DARROW_PLASMA_JAVA_CLIENT=on -DARROW_PLASMA=on -DARROW_DEPENDENCY_SOURCE=BUNDLED -DARROW_GANDIVA_JAVA=ON -DARROW_GANDIVA=ON -DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_BOOST_USE_SHARED=ON -DARROW_JNI=ON -DARROW_WITH_SNAPPY=ON -DARROW_FILESYSTEM=ON -DARROW_JSON=ON -DARROW_WITH_PROTOBUF=ON -DARROW_DATASET=ON -DARROW_WITH_LZ4=ON ..
  make -j
  make install
  cd ../../java
  mvn clean install -q -P arrow-jni -am -Darrow.cpp.build.dir=$current_arrow_path/cpp/release-build/release/ -DskipTests -Dcheckstyle.skip
}


function prepare_intel_conda_arrow() {
  cd $DEV_PATH
  mkdir -p $DEV_PATH/thirdparty/
  cd $DEV_PATH/thirdparty/
  intel_arrow_repo="https://github.com/Intel-bigdata/arrow.git"
  if [ ! -d "arrow" ]; then
    git clone $intel_arrow_repo -b branch-0.17.0-oap-0.9
    cd arrow
  else
    cd arrow
    git pull
  fi
  current_arrow_path=$(pwd)

  cd java/
  mvn clean install -q -P arrow-jni -am -Darrow.cpp.build.dir=/root/miniconda2/envs/oapbuild/lib -DskipTests -Dcheckstyle.skip
}


function prepare_libfabric() {
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  if [ ! -d "libfabric" ]; then
    git clone https://github.com/ofiwg/libfabric.git
  fi
  cd libfabric
  git checkout v1.6.0
  ./autogen.sh

  if [ -z "$ENABLE_RDMA" ]; then
    ENABLE_RDMA=false
  fi

  if $ENABLE_RDMA
  then
      ./configure --disable-sockets --enable-verbs --disable-mlx
  else
      ./configure --disable-sockets --disable-verbs --disable-mlx
  fi

  make -j &&  make install
}

function prepare_HPNL(){
  prepare_libfabric
  yum -y install cmake boost-devel boost-system
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  if [ ! -d "HPNL" ]; then
    git clone https://github.com/Intel-bigdata/HPNL.git
  fi
  cd HPNL
  git checkout origin/spark-pmof-test --track
  git submodule update --init --recursive
  mkdir build
  cd build
  cmake -DWITH_VERBS=ON -DWITH_JAVA=ON ..
  make -j && make install
  cd ../java/hpnl
  mvn  install -DskipTests
}

function prepare_ndctl() {
  yum install -y epel-release which bash-completion
  yum install -y autoconf asciidoctor kmod-devel.x86_64 libudev-devel libuuid-devel json-c-devel jemalloc-devel
  yum groupinstall -y "Development Tools"
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  if [ ! -d "ndctl" ]; then
    git clone https://github.com/pmem/ndctl.git
  fi
  cd ndctl
  git checkout v63
  ./autogen.sh
  ./configure CFLAGS='-g -O2' --prefix=/usr --sysconfdir=/etc --libdir=/usr/lib64
  make -j
  make check
  make install
}

function prepare_PMDK() {
  prepare_ndctl
  yum install -y pandoc
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  if [ ! -d "pmdk" ]; then
    git clone https://github.com/pmem/pmdk.git
  fi
  cd pmdk
  git checkout tags/1.8
  make -j && make install
  export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig/:$PKG_CONFIG_PATH
  echo 'export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig/:$PKG_CONFIG_PATH' > /etc/profile.d/pmdk.sh
  source /etc/profile
}

function prepare_libcuckoo() {
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  git clone https://github.com/efficient/libcuckoo
  cd libcuckoo
  mkdir build
  cd build
  cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DBUILD_EXAMPLES=1 -DBUILD_TESTS=1 ..
  make all && make install
}

function prepare_PMoF() {
  prepare_libfabric
  prepare_HPNL
  prepare_ndctl
  prepare_PMDK
  prepare_libcuckoo
  cd $DEV_PATH
}

function prepare_oneAPI() {
  cd $DEV_PATH/
  cd ../oap-mllib/dev/
  sudo sh install-build-deps-centos.sh
}

function  prepare_conda_build() {
  prepare_maven
  prepare_memkind
  prepare_cmake
  prepare_vmemcache
  prepare_intel_conda_arrow
  prepare_PMoF
  prepare_oneAPI
}

function  prepare_all() {
  prepare_maven
  prepare_memkind
  prepare_cmake
  prepare_vmemcache
  prepare_intel_arrow
  prepare_PMoF
  prepare_oneAPI
}

function oap_build_help() {
    echo " --prepare_maven            function to install Maven"
    echo " --prepare_memkind          function to install Memkind"
    echo " --prepare_cmake            function to install Cmake"
    echo " --install_gcc7             function to install GCC 7.3.0"
    echo " --prepare_vmemcache        function to install Vmemcache"
    echo " --prepare_intel_arrow      function to install intel Arrow"
    echo " --prepare_HPNL             function to install intel HPNL"
    echo " --prepare_PMDK             function to install PMDK "
    echo " --prepare_PMoF             function to install PMoF"
    echo " --prepare_all              function to install all the above"
}

check_jdk
while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --prepare_all)
    shift 1 
    echo "Start to install all compile-time dependencies for OAP ..."
    prepare_all
    exit 0
    ;;
    --prepare_conda_build)
    shift 1
    echo "Start to install all conda compile-time dependencies for OAP ..."
    prepare_conda_build
    exit 0
    ;;
    --prepare_maven)
    shift 1 
    prepare_maven
    exit 0
    ;;
    --prepare_memkind)
    shift 1 
    prepare_memkind
    exit 0
    ;;
    --prepare_cmake)
    shift 1 
    prepare_cmake
    exit 0
    ;;
    --install_gcc7)
    shift 1 
    install_gcc7
    exit 0
    ;;
    --prepare_vmemcache)
    shift 1 
    prepare_vmemcache
    exit 0
    ;;
    --prepare_intel_arrow)
    shift 1 
    prepare_intel_arrow
    exit 0
    ;;
    --prepare_HPNL)
    shift 1 
    prepare_HPNL
    exit 0
    ;;
    --prepare_PMDK)
    shift 1 
    prepare_PMDK
    exit 0
    ;;
    --prepare_PMoF)
    shift 1 
    prepare_PMoF
    exit 0
    ;;
    --prepare_llvm)
    shift 1 
    prepare_llvm
    exit 0
    ;;
    --prepare_llvm)
    shift 1
    prepare_llvm
    exit 0
    ;;
    *)    # unknown option
    echo "Unknown option "
    echo "usage: sh prepare_oap_env.sh [options]"
    echo "Options: "
    oap_build_help
    exit 1
    ;;
esac
done