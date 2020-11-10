#!/bin/bash

export CFLAGS="${CFLAGS} -I${PREFIX}/include -L${PREFIX}/lib"
./autogen.sh
./configure  --prefix=$PREFIX
make
make install

mkdir -p $PREFIX/bin
cp $SRC_DIR/pmdk/bin/* $PREFIX/bin/
mkdir -p $PREFIX/lib
cp $SRC_DIR/pmdk/lib/* $PREFIX/lib/