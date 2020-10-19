# Optimized Analytics Package for Spark\* Platform (OAP)

##### \* LEGAL NOTICE: Your use of this software and any required dependent software (the "Software Package") is subject to the terms and conditions of the software license agreements for the Software Package, which may also include notices, disclaimers, or license terms for third party or open source software included in or with the Software Package, and your use indicates your acceptance of all such terms. Please refer to the "TPP.txt" or other similarly-named text file included with the Software Package for additional details.

##### \* Optimized Analytics Package for Spark* Platform is under Apache 2.0 (https://www.apache.org/licenses/LICENSE-2.0).

OAP is a project to optimize Spark by providing optimized implementation of packages for various aspects including cache, shuffle, native SQL engine, Mllib and so on. In this version, OAP contains the optimized implementations of SQL Index and Data Source Cache supporting DRAM and PMem, RDD Cache PMem Extension, Shuffle Remote PMem Extension,
Remote Shuffle, Intel MLlib, Unified Arrow Data Source and Native SQL Engine.

## Installation Guide
Please follow the link below for the guide to compile and install OAP to your system.
* [OAP Installation Guide](./docs/OAP-Installation-Guide.md)

## User Guide
Please refer to the corresponding documents below for the introductions on how to use the features.

* [SQL Index and Data Source Cache](./oap-cache/oap/README.md)
* [RDD Cache PMem Extension](./oap-spark/README.md)
* [Shuffle Remote PMem Extension](./oap-shuffle/RPMem-shuffle/README.md)
* [Remote Shuffle](./oap-shuffle/remote-shuffle/README.md)
* [Intel MLlib](./oap-mllib/README.md)
* [Unified Arrow Data Source](./oap-data-source/arrow/README.md)
* [Native SQL Engine](./oap-native-sql/README.md)

## Developer Guide
Please follow the link below for the guide for developers.
* [OAP Developer Guide](./docs/Developer-Guide.md)

###### \*Other names and brands may be claimed as the property of others.
