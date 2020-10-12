#!/bin/bash

mkdir build
pushd build
cp -r $RECIPE_DIR/jdkinclude/* $PREFIX/include
cmake -DCMAKE_INSTALL_PREFIX=$PREFIX -DWITH_VERBS=ON ..
make
make install
popd

