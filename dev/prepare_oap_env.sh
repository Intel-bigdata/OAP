#!/bin/bash

MAVEN_TARGET_VERSION=3.6.3

CMAKE_TARGET_VERSION=3.11.1
CMAKE_MIN_VERSION=3.11
TARGET_CMAKE_SOURCE_URL=https://cmake.org/files/v3.11/cmake-3.11.1.tar.gz

if [ -z "$dev_path" ]; then
  dev_path="/tmp"
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

function install_gcc7() {
  #for gcc7
  yum -y install gmp-devel
  yum -y install mpfr-devel
  yum -y install libmpc-devel

  cd $dev_path

  if [ ! -d "gcc-7.3.0" ]; then
    if [ ! -f "llvm-7.0.1.src.tar.xz" ]; then
      wget https://bigsearcher.com/mirrors/gcc/releases/gcc-7.3.0/gcc-7.3.0.tar.xz
    fi
    xz -d gcc-7.3.0.tar.xz
    tar -xvf gcc-7.3.0.tar

  fi
  cd gcc-7.3.0/
  mkdir -p $dev_path/thirdparty/gcc7
  ./configure --prefix=$dev_path/thirdparty/gcc7 --disable-multilib
  make -j
  make install
}

function prepare_llvm() {

  cd $dev_path
  mkdir -p thirdparty/llvm
  cd thirdparty/llvm
  if [ ! -f "llvm-7.0.1.src.tar.xz" ]; then
    wget http://releases.llvm.org/7.0.1/llvm-7.0.1.src.tar.xz
  fi
  tar xf llvm-7.0.1.src.tar.xz
  cd llvm-7.0.1.src/
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

  if [ ! -d "$dev_path/thirdparty/gcc7" ]; then
    install_gcc7
  fi

  export CXX=$dev_path/thirdparty/gcc7/bin/g++
  export CC=$dev_path/thirdparty/gcc7/bin/gcc
  export LD_LIBRARY_PATH=$dev_path/thirdparty/gcc7/lib64:$LD_LIBRARY_PATH

  cmake -DCMAKE_BUILD_TYPE=Release ..
  cmake --build .
  cmake --build . --target install

}

function prepare_ns_arrow() {
  prepare_cmake
  prepare_llvm
  cd $dev_path
  mkdir -p thirdparty/native_sql/
  cd thirdparty/native_sql/
  if [ ! -d "arrow" ]; then
    git clone $intel_arrow_repo -b native-sql-engine-clean
    cd arrow
  else
    cd arrow
    git pull
  fi
  current_arrow_path=$(pwd)
  mkdir -p cpp/release-build

  if [ ! -d "$dev_path/thirdparty/gcc7" ]; then
    install_gcc7
  fi
  export CXX=$dev_path/thirdparty/gcc7/bin/g++
  export CC=$dev_path/thirdparty/gcc7/bin/gcc

  cd cpp/release-build
  cmake -DARROW_GANDIVA_JAVA=ON -DARROW_GANDIVA=ON -DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_BOOST_USE_SHARED=ON -DARROW_JNI=ON -DARROW_WITH_SNAPPY=ON -DARROW_FILESYSTEM=ON -DARROW_JSON=ON ..
  make -j
  make install

  # build java
  cd ../../java
  mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=$current_arrow_path/cpp/release-build/release/ -DskipTests
  # if you are behine proxy, please also add proxy for socks
  # mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=../cpp/release-build/release/ -DskipTests -DsocksProxyHost=${proxyHost} -DsocksProxyPort=1080
}

function prepare_native_sql() {

  #for native-sql
  yum -y install libgsasl
  yum -y install libidn-devel.x86_64
  yum -y install libntlm.x86_64

  intel_spark_repo="https://github.com/Intel-bigdata/spark.git"
  intel_arrow_repo="https://github.com/Intel-bigdata/arrow.git"
  cd $dev_path/thirdparty

  #  prepare spark
  #  if [ ! -d "spark" ]; then
  #    git clone $intel_spark_repo -b native-sql-engine-clean
  #    cd spark
  #    ./build/mvn -Phive -Pyarn -Phadoop-3.2 -Dhadoop.version=3.2.0 -DskipTests clean install
  #    export SPARK_HOME=$(pwd)
  #  fi

  #prepare arrow
  prepare_ns_arrow

  cd $dev_path
  cd ../oap-native-sql/cpp
  mkdir -p build
  cd build/
  if [ ! -d "$dev_path/thirdparty/gcc7" ]; then
    install_gcc7
  fi
  export CXX=$dev_path/thirdparty/gcc7/bin/g++
  export CC=$dev_path/thirdparty/gcc7/bin/gcc
  cmake .. -DTESTS=OFF
  make -j
  make install

  cd $dev_path
  cd ../oap-native-sql/
  mvn clean package -DskipTests

}

function prepare_cmake() {
  CURRENT_CMAKE_VERSION_STR="$(cmake --version)"
  cd $dev_path

  # echo ${CURRENT_CMAKE_VERSION_STR}
  if [[ "$CURRENT_CMAKE_VERSION_STR" == "cmake version"* ]]; then
    echo "cmake is installed"
    array=(${CURRENT_CMAKE_VERSION_STR//,/ })
    CURRENT_CMAKE_VERSION=${array[2]}
    if version_lt $CURRENT_CMAKE_VERSION $CMAKE_MIN_VERSION; then
      echo "$CURRENT_CMAKE_VERSION is less than $CMAKE_MIN_VERSION,install cmake $CMAKE_TARGET_VERSION"
      mkdir -p thirdparty
      cd thirdparty
      echo "$dev_path/thirdparty/cmake-$CMAKE_TARGET_VERSION.tar.gz"
      if [ ! -f "$dev_path/thirdparty/cmake-$CMAKE_TARGET_VERSION.tar.gz" ]; then
        wget $TARGET_CMAKE_SOURCE_URL
      fi
      tar xvf cmake-$CMAKE_TARGET_VERSION.tar.gz
      cd cmake-$CMAKE_TARGET_VERSION/
      ./bootstrap --system-curl
      gmake
      gmake install
      yum remove cmake -y
      ln -s /usr/local/bin/cmake /usr/bin/
      cd $dev_path
    fi
  else
    echo "cmake is not installed"
    mkdir -p thirdparty
    cd thirdparty
    echo "$dev_path/thirdparty/cmake-$CMAKE_TARGET_VERSION.tar.gz"
    if [ ! -f "cmake-$CMAKE_TARGET_VERSION.tar.gz" ]; then
      wget $TARGET_CMAKE_SOURCE_URL
    fi

    tar xvf cmake-$CMAKE_TARGET_VERSION.tar.gz
    cd cmake-$CMAKE_TARGET_VERSION/
    ./bootstrap --system-curl
    gmake
    gmake install
    cd $dev_path
  fi
}

function prepare_memkind() {
  memkind_repo="https://github.com/memkind/memkind.git"
  echo $memkind_repo

  mkdir -p thirdparty
  cd thirdparty
  if [ ! -d "memkind" ]; then
    git clone $memkind_repo
    cd memkind/
  else
    cd memkind/
    git pull
  fi


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
  cd $dev_path

}

function prepare_vmemcache() {
  if [ -n "$(rpm -qa | grep libvmemcache)" ]; then
    echo "libvmemcache is installed"
    return
  fi
  vmemcache_repo="https://github.com/pmem/vmemcache.git"
  prepare_cmake
  cd $dev_path
  mkdir -p thirdparty
  cd thirdparty
  if [ ! -d "vmemcache" ]; then
    git clone $vmemcache_repo
    cd vmemcache
  else
    cd vmemcache
    git pull
  fi

  mkdir -p build
  cd build
  yum -y install rpm-build
  cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DCPACK_GENERATOR=rpm
  make package
  sudo rpm -i libvmemcache*.rpm
}

function prepare_intel_arrow() {
  yum -y install libpthread-stubs0-dev
  yum -y install libnuma-dev

  #install vemecache
  prepare_vmemcache

  #install arrow and plasms
  cd $dev_path/thirdparty
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
