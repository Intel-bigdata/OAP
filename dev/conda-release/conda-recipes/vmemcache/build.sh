#!/bin/bash

mkdir build
pushd build

export CFLAGS="-I$PREFIX/include $CFLAGS"
export LDFLAGS="-L$PREFIX/lib $LDFLAGS"

EXTRA_CMAKE_ARGS=""

# Include g++'s system headers
if [ "$(uname)" == "Linux" ]; then
  SYSTEM_INCLUDES=$(echo | ${CXX} -E -Wp,-v -xc++ - 2>&1 | grep '^ ' | awk '{print "-isystem;" substr($1, 1)}' | tr '\n' ';')
  EXTRA_CMAKE_ARGS=" -DARROW_GANDIVA_PC_CXX_FLAGS=${SYSTEM_INCLUDES}"
fi

cmake -D CMAKE_INSTALL_PREFIX=$PREFIX .. 
make
make install

popd

