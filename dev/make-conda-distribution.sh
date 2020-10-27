#!/bin/bash

OAP_HOME="$(cd "`dirname "$0"`/.."; pwd)"

sh $OAP_HOME/dev/scripts/prepare_oap_env.sh --prepare_conda_build
sh $OAP_HOME/dev/compile-oap.sh
