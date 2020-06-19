#install libpmemobj-cpp
cd /tmp
git clone https://github.com/pmem/libpmemobj-cpp
cd libpmemobj-cpp
mkdir build
cd build
cmake ..
make
sudo make install

#install pmemkv
cd /tmp
git clone https://github.com/pmem/pmemkv
cd pmemkv
mkdir ./build
cd ./build
cmake ..
make
sudo make install

#install pmemkv-java
cd /usr/src/gtest
sudo cmake .
sudo make

cd /tmp
git clone https://github.com/pmem/pmemkv-java.git
cd pmemkv-java
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=/usr/src/gtest
make pmemkv-jni
sudo cp libpmemkv-jni.so /usr/lib
cd ..
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:./build
mvn install -DskipTests