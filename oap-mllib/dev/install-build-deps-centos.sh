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
sudo yum install -y intel-oneapi-daal-devel-2021.1-beta07 intel-oneapi-tbb-devel-2021.1-beta07

echo "Building oneCCL ..."
cd /tmp
rm -rf oneCCL
git clone https://github.com/oneapi-src/oneCCL
cd oneCCL
git checkout -b 2021.1-beta07-1 origin/2021.1-beta07-1
mkdir -p build && cd build
cmake ..
make -j 2 install

#
# Setup building environments manually:
#
# export ONEAPI_ROOT=/opt/intel/inteloneapi
# source /opt/intel/inteloneapi/daal/2021.1-beta07/env/vars.sh
# source /opt/intel/inteloneapi/tbb/2021.1-beta07/env/vars.sh
# source /tmp/oneCCL/build/_install/env/setvars.sh
#
