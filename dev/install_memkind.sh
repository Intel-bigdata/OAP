#install memkind
cd /tmp
git clone https://github.com/memkind/memkind.git
cd memkind && ./build.sh
make
sudo make install