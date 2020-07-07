#gcc $1.cc -std=c++17 -O3 -shared -fPIC /mnt/nvme2/chendi/intel-bigdata/arrow/cpp/release-build/release/libarrow.a -o $1.so
gcc -I /mnt/nvme2/chendi/intel-bigdata/OAP/oap-native-sql/cpp/src/  -I/mnt/nvme2/chendi/intel-bigdata/OAP/oap-native-sql/cpp/src/third_party/sparsehash $1.cc -std=c++17 -O3 -shared -fPIC -larrow -o $1.so
