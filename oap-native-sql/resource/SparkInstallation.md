### Hadoop building from source

``` shell
git clone https://github.com/apache/hadoop.git
cd hadoop
git checkout rel/release-3.2.0
# only build binary for hadoop
mvn clean install -Pdist -DskipTests -Dtar
# build binary and native library such as libhdfs.so for hadoop
# mvn clean install -Pdist,native -DskipTests -Dtar
```

``` shell
export HADOOP_HOME=${HADOOP_PATH}/hadoop-dist/target/hadoop-3.2.0/
```

### [Spark building from source](https://spark.apache.org/docs/latest/building-spark.html)

``` shell
git clone https://github.com/intel-bigdata/spark.git
cd spark && git checkout native-sql-engine-clean
# check spark supported hadoop version
grep \<hadoop\.version\> -r pom.xml
    <hadoop.version>2.7.4</hadoop.version>
    <hadoop.version>3.2.0</hadoop.version>
# so we should build spark specifying hadoop version as 3.2
./build/mvn -Pyarn -Phadoop-3.2 -Dhadoop.version=3.2.0 -DskipTests clean install
```
Specify SPARK_HOME to spark path

``` shell
export SPARK_HOME=${HADOOP_PATH}
```
