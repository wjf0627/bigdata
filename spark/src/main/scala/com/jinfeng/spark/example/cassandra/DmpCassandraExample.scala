package com.jinfeng.spark.example.cassandra

import java.io.Serializable

import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * @package: com.jinfeng.spark.example.cassandra
  * @author: wangjf
  * @date: 2019/3/29
  * @time: 下午12:00
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
class DmpCassandraExample extends Serializable {
  protected def run(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("CassandraExample")
      .master("local")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()
    try {
      val sc = spark.sparkContext

      val path = "/Users/wangjf/Workspace/data/part-00003.snappy.orc"
      val rdd = spark.read.orc(path)
      //  import spark.implicits._
      //  r.getAs("age"), r.getAs("gender"),
      //  rdd.rdd.saveToCassandra()

      rdd.rdd.saveToCassandra("dmp_realtime_service", "dmp_user_features2", SomeColumns("device_id", "age", "gender", "install_apps", "interest", "frequency" overwrite))
      /*
      val df = rdd.rdd.map(r => {
        DeviceInfoCase(r.getAs("device_id"), r.getAs("frenquency"))
      }).filter(r => {
        StringUtils.isNotBlank(r.device_id)
      })

      println(rdd.schema)

       */
      //  val rdd = sc.cassandraTable("dmp", "device_target")
      //  val new_rdd = sc.cassandraTable("test", "message")
      //  rdd.foreach(println)
      //  rdd.joinWithCassandraTable("test", "message").foreach(println)


      //  val a = System.currentTimeMillis
      /*
      val df = sc.parallelize(1 to 10000).map(f = id => {
        //  DeviceOffer("" + UUID.randomUUID(), null, mutable.WrappedArray.empty, null, mutable.WrappedArray.empty, null, mutable.WrappedArray.empty)
        Device("" + UUID.randomUUID())
      })
       */

      //  println(df)
      //  df.saveToCassandra("dmp", "device_target")
      //  df.saveToCassandra("dmp_realtime_service", "dmp_user_features5", SomeColumns("device_id", "frequency" overwrite))

      //  , writeConf = WriteConf(ttl = TTLOption.constant(100)))
      //  val b = System.currentTimeMillis
      //  println("Spark Run Time == " + (b - a))
      //  println(rdd.count)
      //  println(rdd.first)
      //  println(rdd.map(_.getInt("value")).sum)

      //  val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
      //  collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }

  def schema: StructType = {
    StructType(StructField("device_id", StringType) ::
      StructField("age", StringType) ::
      StructField("gender", StringType) ::
      StructField("install", ArrayType(StringType)) ::
      StructField("interest", ArrayType(StringType)) ::
      StructField("frenquency", ArrayType(freSchema)) ::
      Nil)
  }

  def freSchema: StructType = {
    StructType(StructField("tag", StringType) ::
      StructField("cnt", IntegerType) ::
      Nil)
  }
}

object DmpCassandraExample {
  def main(args: Array[String]): Unit = {
    new DmpCassandraExample().run(args)
  }
}