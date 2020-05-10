# OAP Developer Scripts
This directory contains scripts useful to developers when packaging, testing.

## Build OAP

Build the project using the following command and choose build type according to your needs.All jars will generate in path dev/target/
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
