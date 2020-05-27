# OAP Developer Scripts
This directory contains scripts useful to developers when building, testing, and packaging.

## Build OAP

#### Prerequisites for building
Install the required packages on the build system:

- Maven
- [cmake](https://help.directadmin.com/item.php?id=494)
- [Memkind](https://github.com/memkind/memkind)
- [vmemcache](https://github.com/pmem/vmemcache)
- [Intel-arrow](https://github.com/Intel-bigdata/arrow/tree/oap-master)

Use the following command in the `dev` folder  to automatically install these dependencies:

```$xslt
source prepare_oap_env.sh
prepare_all
```

Build the project using the following command. All JARs will generate in path `dev/target/`.

```
sh make-distribution.sh
```