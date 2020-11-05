#!/bin/bash

set -e
set -x
export http_proxy=http://child-prc.intel.com:913
export https_proxy=http://child-prc.intel.com:913
mkdir cpp/build
pushd cpp/build

EXTRA_CMAKE_ARGS=""

# Include g++'s system headers
if [ "$(uname)" == "Linux" ]; then
  SYSTEM_INCLUDES=$(echo | ${CXX} -E -Wp,-v -xc++ - 2>&1 | grep '^ ' | awk '{print "-isystem;" substr($1, 1)}' | tr '\n' ';')
  EXTRA_CMAKE_ARGS=" -DARROW_GANDIVA_PC_CXX_FLAGS=${SYSTEM_INCLUDES}"
fi

cmake \
    -DARROW_PLASMA_JAVA_CLIENT=on \
    -DARROW_PLASMA=ON \
    -DARROW_PACKAGE_PREFIX=$PREFIX \
    -DCMAKE_BUILD_TYPE=release \
    -DCMAKE_INSTALL_LIBDIR=$PREFIX/lib \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_RANLIB=${RANLIB} \
    -DLLVM_TOOLS_BINARY_DIR=$PREFIX/bin \
    -GNinja \
    ${EXTRA_CMAKE_ARGS} \
    ..
ninja install
popd
mkdir -p $PREFIX/oap_jars
cp $SRC_DIR/oap/*.jar $PREFIX/oap_jars/
cp $RECIPE_DIR/libfabric/* $PREFIX/lib/