#! /bin/bash

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
echo $CURRENT_DIR

cd ${CURRENT_DIR}
if [ -d build ]; then
    rm -r build
fi
mkdir build
cd build
cmake ..
make
make
make install

# cd $CURRENT_DIR
# rm -r build
