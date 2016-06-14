The following is to show how to test Spinach manually.
You can paste the following code into Spark-shell.

// ======================== pre import and case class define ========================
import org.apache.spark.{SparkConf, SparkContext}
case class Source(name: String, age: Int, addr: String, phone: String, height: Int)
import sqlContext.implicits._
// ======================== tiny size ================================================
val constant = 10
    val data = sc.parallelize(1 to 5, 5).flatMap { i => {
      val rand = new java.util.Random(System.currentTimeMillis())
      new Iterator[Source] {
              var count = constant
              def hasNext = count > 0
              def next(): Source = {
                count -= 1
                Source("Key" + rand.nextLong(), count + i * constant, "China" + rand.nextInt(), "139" + rand.nextInt(), rand.nextInt() % 500)
              }
            }
          }
        }
sqlContext.createDataFrame(data).write.mode("overwrite").format("spn").save("/user/hive/warehouse/scan/spinachTinySet-uncompress")

val df = sqlContext.read.format("spn").load("/user/hive/warehouse/scan/spinachTinySet-uncompress")
df.selectExpr("count(name)", "count(age)", "count(addr)").show
df.write.mode("overwrite").format("parquet").save("/user/hive/warehouse/scan/parquetTinySet-uncompress")

//  sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
// df.write.mode("overwrite").format("parquet").option("parquet.compression", "UNCOMPRESSED").save("/user/hive/warehouse/scan/parquetTinySet")
val dfP = sqlContext.read.parquet("/user/hive/warehouse/scan/parquetTinySet")
dfP.selectExpr("count(name)", "count(age)", "count(addr)").show


// ==================================== 59.37M/block | total 4G(4.17GB) Size ==============================================
val constant = 750000
    val data = sc.parallelize(1 to 72, 72).flatMap { i => {
      val rand = new java.util.Random(System.currentTimeMillis())
      new Iterator[Source] {
              var count = constant
              def hasNext = count > 0
              def next(): Source = {
                count -= 1
                Source("Key" + rand.nextLong(), count + i * constant, "China" + rand.nextInt(), "139" + rand.nextInt(), rand.nextInt() % 500)
              }
            }
          }
        }
sqlContext.createDataFrame(data).write.mode("overwrite").format("spn").save("/user/hive/warehouse/scan/spinach4GSet-uncompress")
val df = sqlContext.read.format("spn").load("/user/hive/warehouse/scan/spinach4GSet-uncompress")
df.selectExpr("count(name)", "count(age)", "count(addr)").show
sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
df.write.mode("overwrite").format("parquet").save("/user/hive/warehouse/scan/parquet4GSet-uncompress")
val dfP = sqlContext.read.parquet("/user/hive/warehouse/scan/parquet4GSet-uncompress")
dfP.selectExpr("count(name)", "count(age)", "count(addr)").show

// ===================================== 237.48MB/block | total 30G(33.4GB) Size =================================================
val constant = 3000000
    val data = sc.parallelize(1 to 144, 144).flatMap { i => {
      val rand = new java.util.Random(System.currentTimeMillis())
      new Iterator[Source] {
              var count = constant
              def hasNext = count > 0
              def next(): Source = {
                count -= 1
                Source("Key" + rand.nextLong(), count + i * constant, "China" + rand.nextInt(), "139" + rand.nextInt(), rand.nextInt() % 500)
              }
            }
          }
        }
sqlContext.createDataFrame(data).write.mode("overwrite").format("spn").save("/user/hive/warehouse/scan/spinach30GSet-uncompress")
val df = sqlContext.read.format("spn").load("/user/hive/warehouse/scan/spinach30GSet-uncompress")
df.selectExpr("count(name)", "count(age)", "count(addr)").show
sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
df.write.mode("overwrite").format("parquet").save("/user/hive/warehouse/scan/parquet30GSet-uncompress")
val dfP = sqlContext.read.parquet("/user/hive/warehouse/scan/parquet30GSet-uncompress")
dfP.selectExpr("count(name)", "count(age)", "count(addr)").show


// ===================================== 512K index | 237.48MB/block | total 30G(33.4GB) Size =================================================
import org.apache.spark.{SparkConf, SparkContext}
case class Source(name: String, age: Int, addr: String, phone: String, height: Int)
import sqlContext.implicits._

val constant = 3000000
    val data = sc.parallelize(1 to 144, 144).flatMap { i => {
      val rand = new java.util.Random(System.currentTimeMillis())
      new Iterator[Source] {
              var count = constant
              def hasNext = count > 0
              def next(): Source = {
                count -= 1
                Source("Key" + rand.nextLong(), count + i * constant, "China" + rand.nextInt(), "139" + rand.nextInt(), rand.nextInt() % 500)
              }
            }
          }
        }
sqlContext.createDataFrame(data).write.mode("overwrite").format("spn").save("/user/hive/warehouse/scan/rgs512K-spinach30GSet-uncompress-index")
val df = sqlContext.read.format("spn").load("/user/hive/warehouse/scan/rgs512K-spinach30GSet-uncompress-index")
df.registerTempTable("rgs512_30g_index")
sql("create index ageIndex on rgs512_30g_index (age)")
df.filter("age = 6234567").selectExpr("count(name)", "sum(age)").show


// ===================================== 512K no index | 237.48MB/block | total 30G(33.4GB) Size =================================================
import org.apache.spark.{SparkConf, SparkContext}
case class Source(name: String, age: Int, addr: String, phone: String, height: Int)
import sqlContext.implicits._

val constant = 3000000
    val data = sc.parallelize(1 to 144, 144).flatMap { i => {
      val rand = new java.util.Random(System.currentTimeMillis())
      new Iterator[Source] {
              var count = constant
              def hasNext = count > 0
              def next(): Source = {
                count -= 1
                Source("Key" + rand.nextLong(), count + i * constant, "China" + rand.nextInt(), "139" + rand.nextInt(), rand.nextInt() % 500)
              }
            }
          }
        }
sqlContext.createDataFrame(data).write.mode("overwrite").format("spn").save("/user/hive/warehouse/scan/rgs512K-spinach30GSet-uncompress")
val df = sqlContext.read.format("spn").load("/user/hive/warehouse/scan/rgs512K-spinach30GSet-uncompress")
df.filter("age = 6234567").selectExpr("count(name)", "sum(age)").show
df.filter("age >= 6234567 and age <= 6234596").selectExpr("count(name)", "sum(age)").show


// ===================================== 237.48MB/block | total 100G(100.2GB) Size =================================================
val constant = 3000000
    val data = sc.parallelize(1 to 432, 432).flatMap { i => {
      val rand = new java.util.Random(System.currentTimeMillis())
      new Iterator[Source] {
              var count = constant
              def hasNext = count > 0
              def next(): Source = {
                count -= 1
                Source("Key" + rand.nextLong(), count + i * constant, "China" + rand.nextInt(), "139" + rand.nextInt(), rand.nextInt() % 500)
              }
            }
          }
        }
sqlContext.createDataFrame(data).write.mode("overwrite").format("spn").save("/user/hive/warehouse/scan/spinach100GSet-uncompress")
val df = sqlContext.read.format("spn").load("/user/hive/warehouse/scan/spinach100GSet-uncompress")
df.selectExpr("count(name)", "count(age)", "count(addr)").show
sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
df.write.mode("overwrite").format("parquet").save("/user/hive/warehouse/scan/parquet100GSet-uncompress")
val dfP = sqlContext.read.parquet("/user/hive/warehouse/scan/parquet100GSet-uncompress")
dfP.selectExpr("count(name)", "count(age)", "count(addr)").show


// ===================================== row group size 512k | 237.48MB/block | total 100G(100.2GB) Size =================================================
val constant = 3000000
    val data = sc.parallelize(1 to 432, 432).flatMap { i => {
      val rand = new java.util.Random(System.currentTimeMillis())
      new Iterator[Source] {
              var count = constant
              def hasNext = count > 0
              def next(): Source = {
                count -= 1
                Source("Key" + rand.nextLong(), count + i * constant, "China" + rand.nextInt(), "139" + rand.nextInt(), rand.nextInt() % 500)
              }
            }
          }
        }
sqlContext.createDataFrame(data).write.mode("overwrite").format("spn").save("/user/hive/warehouse/scan/rgs512K-Spinach100GSet-uncompress")
val df = sqlContext.read.format("spn").load("/user/hive/warehouse/scan/rgs512K-Spinach100GSet-uncompress")
df.selectExpr("count(name)", "count(age)", "count(addr)").show
df.filter("addr = 'China-953786907'").selectExpr("count(name)", "sum(age)").show
df.filter("addr >= 'China-953786907' and addr <= 'China-977877707'").selectExpr("count(name)", "sum(age)").show
df.filter("age = 6234567").selectExpr("count(name)", "sum(age)").show
df.filter("age >= 6234567 and age <= 6234596").selectExpr("count(name)", "sum(age)").show

// with uncompressed parquet
sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
df.write.mode("overwrite").format("parquet").save("/user/hive/warehouse/scan/rgs512K-Parquet100GSet-uncompress")
val dfP = sqlContext.read.parquet("/user/hive/warehouse/scan/rgs512K-Parquet100GSet-uncompress")
dfP.selectExpr("count(name)", "count(age)", "count(addr)").show
dfP.filter("addr = 'China-953786907'").selectExpr("count(name)", "sum(age)").show
dfP.filter("addr >= 'China-953786907' and addr <= 'China-977877707'").selectExpr("count(name)", "sum(age)").show
dfP.filter("age = 6234567").selectExpr("count(name)", "sum(age)").show
dfP.filter("age >= 6234567 and age <= 6234596").selectExpr("count(name)", "sum(age)").show

//  with gzip parquet
sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
df.write.mode("overwrite").format("parquet").save("/user/hive/warehouse/scan/rgs512K-Parquet100GSet-gzip")
val dfP = sqlContext.read.parquet("/user/hive/warehouse/scan/rgs512K-Parquet100GSet-gzip")
dfP.selectExpr("count(name)", "count(age)", "count(addr)").show
dfP.filter("addr = 'China-953786907'").selectExpr("count(name)", "sum(age)").show
dfP.filter("addr >= 'China-953786907' and addr <= 'China-977877707'").selectExpr("count(name)", "sum(age)").show
dfP.filter("age = 6234567").selectExpr("count(name)", "sum(age)").show
dfP.filter("age >= 6234567 and age <= 6234596").selectExpr("count(name)", "sum(age)").show


/================================================create index===================================================
df.registerTempTable("rgs512_100g")
sql("create index ageIndex on rgs512_100g (age)")
sql("drop index indexName")
sql("select count(name), count(age) from rgs512-100g where age = 6234567")


//================================================cache table ====================================================
df.cache
sql("cache table tablename")

//=========================================key scan==============================================
df.filter("age = 6234567").selectExpr("count(name)", "sum(age)").show
df.filter("age >= 6234567 and age <= 6234596").selectExpr("count(name)", "sum(age)").show
