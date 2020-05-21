#!/bin/bash

# set -e

OAP_HOME="$(cd "`dirname "$0"`/.."; pwd)"

DEV_PATH=$OAP_HOME/dev

source  $DEV_PATH/prepare_oap_env.sh

cd $OAP_HOME
mvn clean -q -Ppersistent-memory -Pvmemcache -DskipTests package
gather