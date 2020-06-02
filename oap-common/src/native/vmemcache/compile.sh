#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -eu
# detect OS
OS="`uname -s`"
case ${OS} in
  'Linux' )
    OS='linux'
    ;;
  'Darwin')
    OS='mac'
    ;;
  *)
    echo "The platform: ${OS} is not supported."
    exit -1
    ;;
esac

# detect Arch
ARCH="`uname -m`"
case ${ARCH} in
   "x86_64")
     ARCH="64"
     ;;
   "i686")
     ARCH="32"
     ;;
   *)
     echo "The arch: ${ARCH} is not supported."
     exit -2
     ;;
esac

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
RESOURCES_DIR=${CURRENT_DIR}/../../resources/${OS}/${ARCH}
echo $RESOURCES_DIR

if [ ! -d ${RESOURCES_DIR}/lib ]; then
    mkdir -p ${RESOURCES_DIR}/lib
fi

cd ${CURRENT_DIR}

make && make clean

set +eu
