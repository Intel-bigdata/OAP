# OAP Developer Scripts
This directory contains scripts useful to developers when packaging, testing.

## Build OAP

#### Prerequisites for building
You need to install the required packages on the build system listed below.
- Maven
- [cmake](https://help.directadmin.com/item.php?id=494)
- [Memkind](https://github.com/memkind/memkind)
- [vmemcache](https://github.com/pmem/vmemcache)
- [Intel-arrow](https://github.com/Intel-bigdata/arrow/tree/oap-master)

You can use the following command  under the folder dev to automatically install these dependencies

```$xslt
source prepare_oap_env.sh
prepare_all
```

Build the project using the following command. All jars will generate in path dev/target/
```
    sh make-distribution.sh
```

## Build native-sql

Build the component of native-sql using the following command and choose build type according to your needs.
```
    sh build-native-sql.sh
```

## About travis

You can use some prepared functions in prepare_oap_env.sh to help you write .travis.yml.
```
    source $dev_path/prepare_oap_env.sh
```
