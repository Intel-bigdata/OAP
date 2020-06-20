#!/bin/bash

# set -e

OAP_HOME="$(cd "`dirname "$0"`/.."; pwd)"

DEV_PATH=$OAP_HOME/dev
OAP_VERSION=0.9.0
SPARK_VERSION=2.4.4

function gather() {
  cd  $DEV_PATH
  target_path=$DEV_PATH/release-package/oap-$OAP_VERSION/jars
  mkdir -p $target_path
  cp ../oap-cache/oap/target/*spark-$SPARK_VERSION.jar $target_path
  cp ../oap-shuffle/remote-shuffle/target/*.jar $target_path
  cp ../oap-common/target/*spark-$SPARK_VERSION.jar $target_path
  cp ../oap-native-sql/core/target/*.jar $target_path
  find $target_path -name "*test*"|xargs rm -rf
  cd $target_path
  rm -f oap-cache-$OAP_VERSION.jar
  cd  $DEV_PATH/release-package
  tar -czf oap-product-$OAP_VERSION-bin-spark-$SPARK_VERSION.tar.gz oap-$OAP_VERSION/
  rm -rf oap-$OAP_VERSION/
  echo "Please check the result in  $DEV_PATH/release-package!"
}

cd $OAP_HOME
mvn clean  -Ppersistent-memory -Pvmemcache -DskipTests package
gather