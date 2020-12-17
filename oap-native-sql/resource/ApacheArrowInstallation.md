# llvm-7.0: 
Arrow Gandiva depends on LLVM, and I noticed current version strictly depends on llvm7.0 if you installed any other version rather than 7.0, it will fail.
``` shell
wget http://releases.llvm.org/7.0.1/llvm-7.0.1.src.tar.xz
tar xf llvm-7.0.1.src.tar.xz
cd llvm-7.0.1.src/
cd tools
wget http://releases.llvm.org/7.0.1/cfe-7.0.1.src.tar.xz
tar xf cfe-7.0.1.src.tar.xz
mv cfe-7.0.1.src clang
cd ..
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j
cmake --build . --target install
# check if clang has also been compiled, if no
cd tools/clang
mkdir build
cd build
cmake ..
make -j
make install
```

# cmake: 
Arrow will download package during compiling, in order to support SSL in cmake, build cmake is optional.
``` shell
wget https://github.com/Kitware/CMake/releases/download/v3.15.0-rc4/cmake-3.15.0-rc4.tar.gz
tar xf cmake-3.15.0-rc4.tar.gz
cd cmake-3.15.0-rc4/
./bootstrap --system-curl --parallel=64 #parallel num depends on your server core number
make -j
make install
cmake --version
cmake version 3.15.0-rc4
```

# Apache Arrow
``` shell
git clone https://github.com/Intel-bigdata/arrow.git
cd arrow && git checkout branch-0.17.0-oap-1.0
mkdir -p arrow/cpp/release-build
cd arrow/cpp/release-build
cmake -DARROW_DEPENDENCY_SOURCE=BUNDLED -DARROW_GANDIVA_JAVA=ON -DARROW_GANDIVA=ON -DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_BOOST_USE_SHARED=ON -DARROW_JNI=ON -DARROW_DATASET=ON -DARROW_WITH_PROTOBUF=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_LZ4=ON -DARROW_FILESYSTEM=ON -DARROW_JSON=ON ..
make -j
make install

# build java
cd ../../java
# change property 'arrow.cpp.build.dir' to the relative path of cpp build dir in gandiva/pom.xml
mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=../cpp/release-build/release/ -DskipTests 
# if you are behine proxy, please also add proxy for socks
mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=../cpp/release-build/release/ -DskipTests -DsocksProxyHost=${proxyHost} -DsocksProxyPort=1080 
```

run test
``` shell
mvn test -pl adapter/parquet -P arrow-jni
mvn test -pl gandiva -P arrow-jni
```

# Copy binary files to oap-native-sql resources directory
Because oap-native-sql plugin will build a stand-alone jar file with arrow dependency, if you choose to build Arrow by yourself, you have to copy below files as a replacement from the original one.
You can find those files in Apache Arrow installation directory or release directory. Below example assume Apache Arrow has been installed on /usr/local/lib64
``` shell
cp /usr/local/lib64/libarrow.so.17 $oap-dir/oap-native-sql/cpp/src/resources
cp /usr/local/lib64/libgandiva.so.17 $oap-dir/oap-native-sql/cpp/src/resources
cp /usr/local/lib64/libparquet.so.17 $oap-dir/oap-native-sql/cpp/src/resources
``` 
