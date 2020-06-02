# OAP Common

OAP commoan package includes native libraries and JNI interface for Intel Optane PMem.

## Prerequisites
Below libraries need to be installed in the machine

- [Memkind](http://memkind.github.io/memkind/)

- [Vmemcache](https://github.com/pmem/vmemcache)

## Building

```
mvn clean package -Ppersistent-memory,vmemcache
```