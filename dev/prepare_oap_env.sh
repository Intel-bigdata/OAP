#!/bin/bash

# set -e
MAVEN_TARGET_VERSION=3.6.3

CMAKE_TARGET_VERSION=3.11.1
CMAKE_MIN_VERSION=3.11
TARGET_CMAKE_SOURCE_URL=https://cmake.org/files/v3.11/cmake-3.11.1.tar.gz

if [ -z "$DEV_PATH" ]; then
  cd $(dirname $BASH_SOURCE)
  DEV_PATH=`echo $(pwd)`
  echo $DEV_PATH
  cd -
fi
echo $DEV_PATH
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

function install_gcc7() {
  #for gcc7
  yum -y install gmp-devel
  yum -y install mpfr-devel
  yum -y install libmpc-devel

  cd $DEV_PATH/thirdparty

  if [ ! -d "gcc-7.3.0" ]; then
    if [ ! -f "llvm-7.0.1.src.tar.xz" ]; then
      wget https://bigsearcher.com/mirrors/gcc/releases/gcc-7.3.0/gcc-7.3.0.tar.xz
    fi
    xz -d gcc-7.3.0.tar.xz
    tar -xvf gcc-7.3.0.tar
  fi

  cd gcc-7.3.0/
  mkdir -p $DEV_PATH/thirdparty/gcc7
  ./configure --prefix=$DEV_PATH/thirdparty/gcc7 --disable-multilib
  make -j
  make install
}

function prepare_llvm() {

  cd $DEV_PATH
  mkdir -p $DEV_PATH/thirdparty/llvm
  cd $DEV_PATH/thirdparty/llvm
  if [ ! -d "llvm-7.0.1.src" ]; then
    if [ ! -f "llvm-7.0.1.src.tar.xz" ]; then
      wget http://releases.llvm.org/7.0.1/llvm-7.0.1.src.tar.xz
    fi
    tar xf llvm-7.0.1.src.tar.xz
  fi
  cd llvm-7.0.1.src/
  mkdir -p tools
  cd tools

  if [ ! -d "clang" ]; then
    if [ ! -f "cfe-7.0.1.src.tar.xz" ]; then
      wget http://releases.llvm.org/7.0.1/cfe-7.0.1.src.tar.xz
      tar xf cfe-7.0.1.src.tar.xz
    fi
    mv cfe-7.0.1.src clang
  fi
  cd ..
  mkdir -p build
  cd build

  if [ ! -d "$DEV_PATH/thirdparty/gcc7" ]; then
    install_gcc7
  fi

  export CXX=$DEV_PATH/thirdparty/gcc7/bin/g++
  export CC=$DEV_PATH/thirdparty/gcc7/bin/gcc
  export LD_LIBRARY_PATH=$DEV_PATH/thirdparty/gcc7/lib64:$LD_LIBRARY_PATH

  cmake -DCMAKE_BUILD_TYPE=Release ..
  cmake --build .
  cmake --build . --target install

}

function prepare_intel_arrow() {
  prepare_cmake
#  prepare_llvm
  cd $DEV_PATH
  mkdir -p $DEV_PATH/thirdparty/
  cd $DEV_PATH/thirdparty/
  intel_arrow_repo="https://github.com/Intel-bigdata/arrow.git"
  if [ ! -d "arrow" ]; then
    git clone $intel_arrow_repo -b oap-master
    cd arrow
  else
    cd arrow
    git pull
  fi
  current_arrow_path=$(pwd)
  mkdir -p cpp/release-build

  if [ ! -d "$DEV_PATH/thirdparty/gcc7" ]; then
    install_gcc7
  fi
  export DEV_PATH=/home/qyao/gitspace/myoap/master/OAP/dev
  export CXX=$DEV_PATH/thirdparty/gcc7/bin/g++
  export CC=$DEV_PATH/thirdparty/gcc7/bin/gcc

  cd cpp/release-build
  cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS="-g -O3" -DCMAKE_CXX_FLAGS="-g -O3"  -DARROW_PLASMA_JAVA_CLIENT=on -DARROW_PLASMA=on -DARROW_DEPENDENCY_SOURCE=BUNDLED -DARROW_GANDIVA_JAVA=ON -DARROW_GANDIVA=ON -DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_BOOST_USE_SHARED=ON -DARROW_JNI=ON -DARROW_WITH_SNAPPY=ON -DARROW_FILESYSTEM=ON -DARROW_JSON=ON -DARROW_WITH_PROTOBUF=ON -DARROW_DATASET=ON ..
  make -j
  make install
}


function prepare_old_intel_arrow() {
  yum -y install libpthread-stubs0-dev
  yum -y install libnuma-dev

  #install vemecache
  prepare_vmemcache

  #install arrow and plasms
  cd $DEV_PATH/thirdparty
  if [ ! -d "arrow" ]; then
    git clone https://github.com/Intel-bigdata/arrow.git -b oap-master
    cd arrow
  else
    cd arrow
    git pull
  fi

  cd cpp
  rm -rf release
  mkdir -p release
  cd release

  #build libarrow, libplasma, libplasma_java
  cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS="-g -O3" -DCMAKE_CXX_FLAGS="-g -O3" -DARROW_BUILD_TESTS=on -DARROW_PLASMA_JAVA_CLIENT=on -DARROW_PLASMA=on -DARROW_DEPENDENCY_SOURCE=BUNDLED ..
  make -j$(nproc)
  make install -j$(nproc)
}

function prepare_native_sql_cpp() {

  #for native-sql
  yum -y install libgsasl
  yum -y install libidn-devel.x86_64
  yum -y install libntlm.x86_64

  intel_spark_repo="https://github.com/Intel-bigdata/spark.git"
  intel_arrow_repo="https://github.com/Intel-bigdata/arrow.git"
  cd $DEV_PATH/thirdparty

  prepare_ns_arrow

  cd $DEV_PATH
  cd ../oap-native-sql/cpp
  mkdir -p build
  cd build/
  if [ ! -d "$DEV_PATH/thirdparty/gcc7" ]; then
    install_gcc7
  fi
  export CXX=$DEV_PATH/thirdparty/gcc7/bin/g++
  export CC=$DEV_PATH/thirdparty/gcc7/bin/gcc
  cmake .. -DTESTS=OFF
  make -j
  make install

}

function prepare_libfabric() {
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  git clone https://github.com/ofiwg/libfabric.git
  cd libfabric
  git checkout v1.6.0
  ./autogen.sh
  ./configure --disable-sockets --enable-verbs --disable-mlx
  make -j &&  make install
}

function prepare_HPNL(){
  yum -y install cmake boost-devel boost-system
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  git clone https://github.com/Intel-bigdata/HPNL.git
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
  yum install -y autoconf asciidoctor kmod-devel.x86_64 libudev-devel libuuid-devel jsonc-devel jemalloc-devel
  yum groupinstall -y "Development Tools"
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  git clone https://github.com/pmem/ndctl.git
  cd ndctl
  git checkout v63
  ./autogen.sh
  ./configure CFLAGS='-g -O2' --prefix=/usr --sysconfdir=/etc --libdir=/usr/lib64
  make -j
  make check
  make install
  cd ../java/hnpl
  mvn install
}

function prepare_PMDK() {
  yum install -y pandoc
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  git clone https://github.com/pmem/pmdk.git
  cd pmdk
  git checkout tags/1.8
  make -j && make install
  export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig/:$PKG_CONFIG_PATH
  echo 'export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig/:$PKG_CONFIG_PATH' > /etc/profile.d/pmdk.sh
  echo 'export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig/:$PKG_CONFIG_PATH' > ~/.bashrc
  source ~/.bashrc
}

function prepare_libcuckoo() {
  mkdir -p $DEV_PATH/thirdparty
  cd $DEV_PATH/thirdparty
  git clone https://github.com/efficient/libcuckoo
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
  cd $DEV_PATH/thirdparty
  git clone https://github.com/Intel-bigdata/Spark-PMoF.git
  cd Spark-PMoF
  mvn install -DskipTests
}

function  prepare_all() {
  prepare_maven
  prepare_memkind
  prepare_cmake
  prepare_vmemcache
  prepare_native_sql
}

function oap_build_help() {
    echo " prepare_maven          = function to install Maven"
    echo " prepare_memkind         = function to install Memkind"
    echo " prepare_cmake           = function to install Cmake"
    echo " prepare_vmemcache       = function to install Vmemcache"
    echo " prepare_native_sql_cpp  = function to install Native Sql Cpp"
    echo " prepare_all             = function to install all the above"
}