#install arrow and plasms
cd /tmp
git clone https://github.com/Intel-bigdata/arrow.git
cd arrow && git checkout oap-master
cd cpp
rm -rf release
mkdir release
cd release
#build libarrow, libplasma, libplasma_java
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS="-g -O3" -DCMAKE_CXX_FLAGS="-g -O3" -DARROW_BUILD_TESTS=on -DARROW_PLASMA_JAVA_CLIENT=on -DARROW_PLASMA=on -DARROW_DEPENDENCY_SOURCE=BUNDLED ..
make -j$(nproc)
sudo make install -j$(nproc)
cd ../../java
mvn clean -q -DskipTests install