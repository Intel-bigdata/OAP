#install vemecache
cd /tmp
git clone https://github.com/pmem/vmemcache.git
pushd vmemcache
mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DCPACK_GENERATOR=deb
make package
sudo dpkg -i libvmemcache*.deb
popd