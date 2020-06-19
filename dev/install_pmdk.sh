#install ndctl
cd /tmp
git clone https://github.com/pmem/ndctl.git
cd ndctl
./autogen.sh
./configure CFLAGS='-g -O2' --prefix=/usr --sysconfdir=/etc --libdir=/usr/lib
make
sudo make install

#install pmdk
cd /tmp
git clone https://github.com/pmem/pmdk.git
cd pmdk
make
sudo make install