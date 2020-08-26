#!/usr/bin/env bash

echo "Installing oneAPI components ..."
cd /tmp
tee > /tmp/oneAPI.repo << EOF
[oneAPI]
name=Intel(R) oneAPI repository
baseurl=https://yum.repos.intel.com/oneapi
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://yum.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2023.PUB
EOF
sudo mv /tmp/oneAPI.repo /etc/yum.repos.d
sudo yum install intel-oneapi-daal-devel-2021.1-beta08 intel-oneapi-tbb-devel-2021.1-beta08

echo "Building oneCCL ..."
cd /tmp
git clone https://github.com/oneapi-src/oneCCL
cd oneCCL
git checkout -b beta08 origin/beta08
mkdir build && cd build
cmake ..
make -j 2 install

#
# Setup building environments manually:
#
# export ONEAPI_ROOT=/opt/intel/oneapi
# source $ONEAPI_ROOT/daal/2021.1-beta08/env/vars.sh
# source $ONEAPI_ROOT/tbb/2021.1-beta08/env/vars.sh
# source /tmp/oneCCL/build/_install/env/setvars.sh
#
