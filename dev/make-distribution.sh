#!/bin/bash

# set -e

OAP_HOME="$(cd "`dirname "$0"`/.."; pwd)"

DEV_PATH=$OAP_HOME/dev
OAP_VERSION=0.8.0

function gather() {
  cd  $DEV_PATH
  mkdir -p oap_jars
  cp ../oap-cache/oap/target/*.jar $DEV_PATH/oap_jars/
  cp ../oap-shuffle/remote-shuffle/target/*.jar $DEV_PATH/oap_jars/
  cp ../oap-common/target/*.jar $DEV_PATH/oap_jars/
  find $DEV_PATH/oap_jars -name "*test*"|xargs rm -rf
  cd oap_jars
  rm -f oap-cache-$OAP_VERSION.jar
  tar -czf oap-$OAP_VERSION.tar.gz *.jar
  echo "Please check the result in  $DEV_PATH/oap_jars!"
}

cd $OAP_HOME
mvn clean  -Ppersistent-memory -Pvmemcache -DskipTests package
gather