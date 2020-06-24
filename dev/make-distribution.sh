#!/bin/bash
set -e

OAP_HOME="$(cd "`dirname "$0"`/.."; pwd)"

DEV_PATH=$OAP_HOME/dev
OAP_VERSION=0.8.0
SPARK_VERSION=2.4.4
function gather() {
  cd  $DEV_PATH
  package_name=oap-product-$OAP_VERSION-bin-spark-$SPARK_VERSION
  target_path=$DEV_PATH/release-package/$package_name/jars
  mkdir -p $target_path
  cp ../oap-cache/oap/target/*.jar $target_path
  cp ../oap-shuffle/remote-shuffle/target/*.jar $target_path
  cp ../oap-common/target/*.jar $target_path
  cp ../oap-spark/target/*.jar $target_path
  cp ../oap-shuffle/RPMem-shuffle/core/target/*.jar $target_path
  find $target_path -name "*test*"|xargs rm -rf
  cd $target_path
  rm -f oap-cache-$OAP_VERSION.jar
  cd  $DEV_PATH/release-package
  tar -czf $package_name.tar.gz $package_name/
  echo "Please check the result in  $DEV_PATH/release-package!"
}

cd $OAP_HOME
export CXX=$DEV_PATH/thirdparty/gcc7/bin/g++
export CC=$DEV_PATH/thirdparty/gcc7/bin/gcc
mvn clean  -Ppersistent-memory -Pvmemcache -DskipTests package
gather