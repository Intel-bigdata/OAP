#!/bin/bash

# set -e

OAP_HOME="$(cd "`dirname "$0"`/.."; pwd)"

DEV_PATH=$OAP_HOME/dev
OAP_VERSION=0.8.4
SPARK_VERSION=2.4.4

GCC_MIN_VERSION=7.0


function version_lt() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" != "$1"; }

function version_ge() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$1"; }


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
        wget https://bigsearcher.com/mirrors/gcc/releases/gcc-7.3.0/gcc-7.3.0.tar.xz
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
  fi
}

function gather() {
  cd  $DEV_PATH
  package_name=oap-$OAP_VERSION-bin-spark-$SPARK_VERSION
  target_path=$DEV_PATH/release-package/$package_name/jars
  rm -rf $target_path
  mkdir -p $target_path
  cp ../oap-cache/oap/target/*.jar $target_path
  cp ../oap-shuffle/remote-shuffle/target/*.jar $target_path
  cp ../oap-common/target/*.jar $target_path
  cp ../oap-spark/target/*.jar $target_path
  cp ../oap-shuffle/RPMem-shuffle/core/target/*.jar $target_path
  cp ../dev/thirdparty/arrow/java/plasma/target/arrow-plasma-0.17.0.jar $target_path
  find $target_path -name "*test*"|xargs rm -rf
  cd $target_path
  rm -f oap-cache-$OAP_VERSION.jar
  mkdir -p $DEV_PATH/thirdparty/arrow/oap
  rm -rf $DEV_PATH/thirdparty/arrow/oap/*
  cp $target_path/* $DEV_PATH/thirdparty/arrow/oap/
  cd  $DEV_PATH/release-package
  tar -czf $package_name.tar.gz $package_name/
  echo "Please check the result in  $DEV_PATH/release-package!"
}

function help() {
    echo " Options: "
    echo " --oap-cache           "
    echo " --remote-shuffle      "
    echo " --oap-rpmem-shuffle   "
    echo " --oap-spark           "
}

check_gcc
cd $OAP_HOME
while [[ $# -ge 0 ]]
do
key="$1"
case $key in
    "")
    shift 1
    echo "Start to compile all modules of OAP ..."
    cd $OAP_HOME
    mvn clean  -Ppersistent-memory -Pvmemcache -DskipTests package
    gather
    exit 0
    ;;
    --oap-cache)
    shift 1
    mvn clean package -pl com.intel.oap:oap-cache -am -Ppersistent-memory -Pvmemcache -DskipTests
    exit 0
    ;;
    --remote-shuffle)
    shift 1
    mvn clean package -pl com.intel.oap:oap-remote-shuffle  -am -DskipTests
    exit 0
    ;;
    --oap-rpmem-shuffle)
    shift 1
    cd $OAP_HOME/oap-shuffle/RPMem-shuffle
    mvn clean package -DskipTests
    cd $OAP_HOME
    exit 0
    ;;
    --oap-spark)
    shift 1
    mvn clean package -pl com.intel.oap:oap-spark -Ppersistent-memory  -am -DskipTests
    exit 0
    ;;
    --help)
    shift 1
    help
    exit 0
    ;;
    *)    # unknown option
    echo "Unknown option "
    echo "usage: sh compile-oap.sh [options]"
    help
    exit 1
    ;;
esac
done


