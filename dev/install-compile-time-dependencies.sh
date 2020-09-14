#!/bin/bash

OAP_HOME="$(cd "`dirname "$0"`/.."; pwd)"

while [[ $# -ge 0 ]]
do
key="$1"
case $key in
    --with-rdma)
    shift 1
    echo "Start to install all compile-time dependencies for OAP ..."
    export ENABLE_RDMA=true
    sh $OAP_HOME/dev/scripts/prepare_oap_env.sh --prepare_all
    exit 0
    ;;
    "")
    shift 1
    echo "Start to install all compile-time dependencies for OAP ..."
    sh $OAP_HOME/dev/scripts/prepare_oap_env.sh --prepare_all
    exit 0
    ;;
    *)    # unknown option
    echo "Unknown option "
    exit 1
    ;;
esac
done