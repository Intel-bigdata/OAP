#!/bin/bash

mkdir build
pushd build
cp -r $RECIPE_DIR/jdkinclude/* $PREFIX/include
cmake -DCMAKE_INSTALL_PREFIX=$PREFIX -DWITH_VERBS=ON ..
make
make install
popd

mkdir -p $PREFIX/bin
cp $SRC_DIR/pmdk/bin/* $PREFIX/bin/
mkdir -p $PREFIX/lib
cp $SRC_DIR/pmdk/lib/* $PREFIX/lib/