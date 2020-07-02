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
git clone https://github.com/pmem/pmemkv-java.git -b stable-1.0
cd pmemkv-java
mvn install -DskipTests
sudo cp src/main/cpp/target/libpmemkv-jni.so /usr/lib