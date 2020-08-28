#!/bin/bash

CUR_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
OAP_HOME=$(dirname $CUR_DIR)
PATCH_DIR=$OAP_HOME/patches

GIT=$(command -v git)
check_git_exist() {
  if ! [ -x "$GIT" ]; then
    echo "ERROR: git is not found!"
    exit 1
  fi
}
patch_to_oap() {
  check_git_exist
  cd $OAP_HOME
  echo '##########'
  echo  $OAP_HOME
#  if ! [ -d .git ]; then
#    $GIT init
#  fi

  $GIT checkout -- src/main/spark2.3.2/scala/org/apache/spark/SparkEnv.scala
  $GIT apply $PATCH_DIR/OAP-SparkEnv-numa-binding.patch
  if [ $? != 0 ]; then
    echo "Fail to apply the patch to oap. Please check if you have already patched."
  else
    echo "Apply patches to oap successfully."
  fi
}

patch_to_oap
