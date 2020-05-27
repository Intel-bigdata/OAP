#!/bin/bash

# set -e

OAP_HOME="$(cd "`dirname "$0"`/.."; pwd)"

DEV_PATH=$OAP_HOME/dev

function gather() {
  cd  $DEV_PATH
  mkdir -p target
  cp ../oap-cache/oap/target/*.jar $DEV_PATH/target/
  cp ../oap-shuffle/remote-shuffle/target/*.jar $DEV_PATH/target/
  cp ../oap-common/target/*.jar $DEV_PATH/target/
  find $DEV_PATH/target -name "*test*"|xargs rm -rf
  echo "Please check the result in  $DEV_PATH/target!"
}

cd $OAP_HOME
mvn clean  -Ppersistent-memory -Pvmemcache -DskipTests package
gather