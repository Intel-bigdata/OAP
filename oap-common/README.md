# OAP Common

OAP commoan package includes native libraries and JNI interface for Intel Optane PMem.

## Prerequisites
Below libraries need to be installed in the machine

- [Memkind](https://github.com/memkind/memkind/tree/v1.10.1-rc2)

- [Vmemcache](https://github.com/pmem/vmemcache)

## Building

```
mvn clean package -Ppersistent-memory,vmemcache
```