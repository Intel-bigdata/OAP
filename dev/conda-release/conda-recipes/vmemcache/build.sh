#!/bin/bash

mkdir build
pushd build

export CFLAGS="-I$PREFIX/include $CFLAGS"
export LDFLAGS="-L$PREFIX/lib $LDFLAGS"



cmake -DCMAKE_INSTALL_PREFIX=$PREFIX ..
make
make install
popd

