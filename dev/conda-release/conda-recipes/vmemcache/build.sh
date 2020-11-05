#!/bin/bash

mkdir build
pushd build

export CFLAGS="-I$PREFIX/include $CFLAGS"
export LDFLAGS="-L$PREFIX/lib $LDFLAGS"
mkdir -p $PREFIX/lib


cmake -DCMAKE_INSTALL_PREFIX=$PREFIX ..
make
make install

cp $PREFIX/lib64/libvmemcache.so* $PREFIX/lib
popd

