#!/bin/bash

export CFLAGS="${CFLAGS} -I${PREFIX}/include -L${PREFIX}/lib"
./autogen.sh
./configure  --prefix=$PREFIX
make
make install
