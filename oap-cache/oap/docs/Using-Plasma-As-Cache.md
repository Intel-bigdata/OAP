# OAP- How to use Plasma
## Introduction
- OAP use Plasma as a node-level external cache service, the benefit of using external cache is data can be shared across process boundaries. [Plasma](http://arrow.apache.org/blog/2017/08/08/plasma-in-memory-object-store/) is a high-performance shared-memory object store, it's a component of [Apache Arrow](https://github.com/apache/arrow). We have modified Plasma to support Intel Optane PMem, and open source on [Intel-bigdata Arrow](https://github.com/Intel-bigdata/arrow/tree/oap-master) repo. Plasma cache Architecture shown as following figure. A Spark executor contains one or more Plasma client, clients communicate with Plasma Store server via unix domain socket on local node, data can be shared through shared memory across multi executors. Data will be cached in shared memory first, and plasma store will evict data to Intel Optane PMem since PMem has larger capacity.   
 
![Plasma_Architecture](./image/plasma.png)


## How to build
### what you need 
To use optimized Plasma cache with OAP, you need following components:  
    (1) libarrow.so, libplasma.so, libplasma_jni.so: dynamic libraries, will be used in plasma client.   
    (2) plasma-store-server: executable file, plasma cache service.  
    (3) arrow-plasma-0.17.0.jar: will be used when compile oap and spark runtime also need it. 
    
(1) and (2) will be provided as rpm package, we provide [Fedora 29](https://github.com/Intel-bigdata/arrow/releases/download/apache-arrow-0.17.0-intel-oap-0.8/arrow-plasma-intel-libs-0.17.0-1.fc29.x86_64.rpm) and [Cent OS 7.6](https://github.com/Intel-bigdata/arrow/releases/download/apache-arrow-0.17.0-intel-oap-0.8/arrow-plasma-intel-libs-0.17.0-1.el7.x86_64.rpm) rpm package, you can download it on release page.
Run `rpm -ivh arrow-plasma-intel-libs-0.17.0-1*x86_64.rpm` to install it.   
(3) will be provided in maven central repo. You need to download [it](https://repo1.maven.org/maven2/com/intel/arrow/arrow-plasma/0.17.0/arrow-plasma-0.17.0.jar) and copy to `$SPARK_HOME/jars` dir.

   
### build Plasma related files manually
#### so file and binary file  
  clone code from Intel-arrow repo and run following commands, this will install libplasma.so, libarrow.so, libplasma_jni.so and plasma-store-server to your system path(/usr/lib64 by default). And if you are using spark in a cluster environment, you can copy these files to all nodes in your cluster if os or distribution are same, otherwise, you need compile it on each node.
  
```
cd /tmp
git clone https://github.com/Intel-bigdata/arrow.git
cd arrow && git checkout oap-master
cd cpp
mkdir release
cd release
#build libarrow, libplasma, libplasma_java
cmake -DCMAKE_INSTALL_PREFIX=/usr/ -DCMAKE_BUILD_TYPE=Release -DARROW_BUILD_TESTS=on -DARROW_PLASMA_JAVA_CLIENT=on -DARROW_PLASMA=on -DARROW_DEPENDENCY_SOURCE=BUNDLED  ..
make -j$(nproc)
sudo make install -j$(nproc)
```

#### arrow-plasma-0.17.0.jar  
   change to arrow repo java direction, run following command, this will install arrow jars to your local maven repo, and you can compile oap-cache module package now. Beisdes, you need copy arrow-plasma-0.17.0.jar to `$SPARK_HOME/jars/` dir, cause this jar is needed when using external cache.
   
```
cd $ARROW_REPO_DIR/java
mvn clean -q -pl plasma -DskipTests install
```

## How to Run Spark-sql with Plasma

### config files:
Follow [User guide](User-Guide.md), and you should update config file `spark-default.conf` as follow:

For Parquet data format, provides the following conf options:

```
spark.sql.oap.parquet.data.cache.enable                    true 
spark.oap.cache.strategy                                   external
spark.sql.oap.cache.guardian.memory.size                   10g      # according to your cluster
spark.sql.oap.cache.external.client.pool.size              10
```

For Orc data format, provides following conf options:

```
spark.sql.orc.copyBatchToSpark                             true 
spark.sql.oap.orc.data.cache.enable                        true 
spark.oap.cache.strategy                                   external 
spark.sql.oap.cache.guardian.memory.size                   10g      # according to your cluster
spark.sql.oap.cache.external.client.pool.size              10
```


#### Start plasma service manually

 plasma config parameters:  
 ```
 -m  how much Bytes share memory plasma will use
 -s  Unix Domain sockcet path
 -e  using external store
     vmemcache: using vmemcahe as external store
     propertyFilePath: It's recommended to use propertyFilePath to pass parameters.
     Or you can write these parameters directly in your starting command. Use "?" to seperate different numaNodes.
 ```

You can start plasma service on each node as following command, and then you can run your workload.

```
plasma-store-server -m 15000000000 -s /tmp/plasmaStore -e vmemcache://propertyFilePath:/tmp/persistent-memory.properties  
```
or 
``` 
plasma-store-server -m 15000000000 -s /tmp/plasmaStore -e vmemcache://totalNumaNodeNum:2,\
numaNodeId1:1,initialPath1:/mnt/pmem0,requiredSize1:15000000,readPoolSize1:12,writePoolSize1:12\
?numaNodeId2:2,initialPath2:/mnt/pmem1,requiredSize2:15000000,readPoolSize2:12,writePoolSize2:12
```

An example persistent-memory.properties:

```
  # Example
  totalNumaNodeNum = 2
    
  numaNodeId1 = 1
  initialPath1 = /mnt/pmem0
  requiredSize1 = 15000000
  readPoolSize1 = 12 
  writePoolSize1 = 12
    
  numaNodeId2 = 2
  initialPath2 = /mnt/pmem1
  requiredSize2 = 15000000
  readPoolSize2 = 12 
  writePoolSize2 = 12
```

```requiredSize readPoolSize writePoolSize``` is optional,will use default value if you don't pass these three parameters.
But please remember to pass ```totalNumaNodeNum``` and ```initialPath```.

*Please note that parameters in the command will cover parameters in persistent-memory.properties.*

 Remember to kill `plasma-store-server` process if you no longer need cache, and you should delete `/tmp/plasmaStore` which is a Unix domain socket.  
  
#### Use yarn to start plamsa service
We can use yarn(hadoop version >= 3.1) to start plasma service, you should provide a json file like following.
```
{
  "name": "plasma-store-service",
  "version": 1,
  "components" :
  [
   {
     "name": "plasma-store-service",
     "number_of_containers": 3,
     "launch_command": "plasma-store-server -m 15000000000 -s /tmp/plasmaStore -e vmemcache://propertyFilePath:/tmp/persistent-memory.properties ",
     "resource": {
       "cpus": 1,
       "memory": 512
     }
   }
  ]
}
```

Run command  ```yarn app -launch plasma-store-service /tmp/plasmaLaunch.json``` to start plasma server.  
Run ```yarn app -stop plasma-store-service``` to stop it.  
Run ```yarn app -destroy plasma-store-service```to destroy it.
