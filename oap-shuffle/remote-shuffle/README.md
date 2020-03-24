#Get Started with Spark Remote Shuffle

##Build and Deploy Remote Shuffle Plugin

Build the project using the following command or download the pre-built jar: remote-shuffle-1.0.jar. This file needs to
be deployed on every compute node that runs Spark. Place it in a directory that is accessible to all the nodes or copy
it to every node.


    mvn -DskipTests clean package
    
##Enable Remote Shuffle

To enable remote shuffle in Spark, add the jar files to the classpath of the Spark executor and driver. You can add the
following configurations in spark-defaults.conf or Spark submit command line arguments. As we also use DAOS Hadoop
filesystem, the DAOS Hadoop jars should also be included in the classpath.


    spark.executor.extraClassPath  /path/to/daos-java-api-<version>.jar: /path/to/hadoop-daos-<version>.jar:
                                                        /path/to/remote-shuffle-dir/remote-shuffle-<version>.jar
    spark.driver.extraClassPath    /path/to/daos-java-api-<version>.jar: /path/to/hadoop-daos-<version>.jar:
                                                        /path/to/remote-shuffle-dir/remote-shuffle-<version>.jar

Next, enable the remote shuffle manager and specify the DAOS Hadoop file system URI that are used to store the shuffle
data.


    spark.shuffle.manager                      org.apache.spark.shuffle.remote.RemoteShuffleManager
    spark.shuffle.remote.storageMasterUri      daos://default:1

##Remote Shuffle Configurations

This section describes a few configurations in remote shuffle that change the behavior of remote shuffle. Most of the
configurations should work well using the default values. Most shuffle configurations in Spark are still supported in
remote shuffle like “spark.shuffle.compress”. Those configurations are available in Spark’s document.

###Shuffle Root Directory

This is to configure the root directory for storing remote shuffle files in DAOS. For each Spark application, a
directory named by Spark application ID is created under this root directory.

    spark.shuffle.remote.filesRootDirectory    /shuffle
                
###Index cache size

This is to configure the cache size for shuffle index files in every executor. Shuffle data includes data files and
index files. An index file is small but will be read many (the number of reducers) times. On a large scale, constantly
reading these small index files from DAOS is going to cause much overhead and latency. In addition, the shuffle files’
transfer completely relies on the network between compute nodes and DAOS nodes. But the network inside compute nodes are
not fully utilized. The index cache can eliminate the overhead of reading index files from DAOS multiple times. By
enabling index file cache, a reduce task fetches them from the remote executors who write them instead of reading from
DAOS. If the remote executor doesn’t have a desired index file in its cache, it will read the file from DAOS and cache
it locally. The feature can also be disabled by setting the value to zero.


    spark.shuffle.remote.index.cache.size                                                           30m


