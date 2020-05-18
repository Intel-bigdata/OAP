#!/bin/bash

# set -e


dev_path=$(pwd)

source $dev_path/prepare_oap_env.sh

function gather() {
  cd $dev_path
  mkdir -p target
  cp ../oap-cache/oap/target/*.jar target/
  cp ../oap-shuffle/remote-shuffle/target/*.jar target/
  cp ../oap-common/target/*.jar target/
  echo "Please check the result in $dev_path/target !"
}

prepare_maven
prepare_memkind
prepare_cmake
prepare_vmemcache
prepare_intel_arrow
cd $dev_path
cd ..
mvn clean -q -Ppersistent-memory -Pvmemcache -DskipTests package
gather