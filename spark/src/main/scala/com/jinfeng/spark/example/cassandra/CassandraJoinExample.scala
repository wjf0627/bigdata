package com.jinfeng.spark.example.cassandra

import java.io.Serializable
import java.util

import com.datastax.spark.connector.{SomeColumns, _}
import com.jinfeng.spark.example.entity.DeviceTarget
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
  * @package: com.jinfeng.spark.example.cassandra
  * @author: wangjf
  * @date: 2019/3/29
  * @time: 下午12:00
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
class CassandraJoinExample extends Serializable {

  var offer_id = ""

  protected def run(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("CassandraExample")
      .master("local[10]")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()
    try {

      val sc = spark.sparkContext

      offer_id = "1515668448"
      val array: util.Set[Int] = new util.HashSet[Int]()
      offer_id.split(",").foreach(r => {
        array.add(Integer.parseInt(r))
      })
      val input = "/Users/wangjf/Workspace/data/1515668448.txt"
      val rdd = sc.textFile(input)
      import spark.implicits._
      val dfo = rdd.map(r => {
        r
      }).toDF("device_id")

      dfo.createOrReplaceTempView("device")


      /*
      val update_sql =
        s"""
           |UPDATE dmp_realtime_service.dmp_user_features SET target_offer_list = target_offer_list - {${offer_id}} WHERE target_offer_list CONTAINS ${offer_id}
        """.stripMargin
      CassandraConnector(sc).withSessionDo {
        session => {
          session.execute(update_sql)
        }
      }
       */

      //  df.saveToCassandra("dmp_realtime_service", "dmp_user_features", SomeColumns("device_id", "target_offer_list" overwrite))




      //  options = new mutable.HashMap[String, String]()
      //  options.put("keyspace", "test")
      //  options.put("table", "device")
      //  val dss = spark.read.format("org.apache.spark.sql.cassandra").options(options).load
      //  dss.createOrReplaceTempView("device")

      //  ds.schema.foreach(println)
      //  val df = spark.sql("SELECT a.device_id,b.device_id,coalesce(target_offer_list,array()) as target_offer_list,age,gender,behavior,install_apps,interest FROM device_target a FULL OUTER JOIN device b ON a.device_id = b.device_id")

      val df = spark.sql("SELECT a.device_id,b.device_id,coalesce(target_offer_list,array()) as target_offer_list FROM device_target a FULL OUTER JOIN device b ON a.device_id = b.device_id")
        .persist(StorageLevels.MEMORY_AND_DISK_SER)

      val fdf = df.where(s"a.device_id IS NOT NULL AND b.device_id IS NULL AND ARRAY_CONTAINS(target_offer_list,'${offer_id}')")

      val tdf = df.where(s"!(a.device_id IS NOT NULL AND b.device_id IS NULL) AND !ARRAY_CONTAINS(target_offer_list,'${offer_id}')")

      //  val tt = df.where("(a.device_id IS NOT NULL AND b.device_id IS NOT NULL) OR ")
      //  val ft = df.where("a.device_id IS NULL AND b.device_id IS NOT NULL")

      //  val dff = tt.union(ft).select(coalesce(col("a.device_id"), col("b.device_id")) as "device_id", col("target_offer_list"), col("age"),
      //  col("behavior"), col("gender"), col("install_apps"), col("interest"))

      //  DeviceOffer(r.getAs("device_id"), r.getAs("age"), r.getAs("behavior"), r.getAs("gender"), r.getAs("install_apps"), r.getAs("interest"), r.getAs("new_target_offer_list"))

      val ff = fdf.select(coalesce(col("a.device_id"), col("b.device_id")) as "device_id", col("target_offer_list"))
        .withColumn("new_target_offer_list", subValue(col("target_offer_list")))
        .rdd.map(r => {
        DeviceTarget(r.getAs("device_id"), r.getAs("new_target_offer_list"))
      })

      //  ff.saveToCassandra("dmp", "device_target", SomeColumns("device_id", "target_offer_list" overwrite))

      val tf = tdf.select(coalesce(col("a.device_id"), col("b.device_id")) as "device_id", col("target_offer_list"))
        .withColumn("new_target_offer_list", addValue(col("target_offer_list")))
        .rdd.map(r => {
        DeviceTarget(r.getAs("device_id"), r.getAs("new_target_offer_list"))
      })

      ff.union(tf).saveToCassandra("dmp_realtime_service", "dmp_user_features", SomeColumns("device_id", "target_offer_list" overwrite))

      //  spark.udf.register("addValue", addValue(_, offer))
      //  tt.withColumn("new_target_offer_list", addValue(col("target_offer_list"))).show
      //      tt.show
      //  val tf = df.where("a.device_id IS NOT NULL AND b.device_id IS NULL")
      //  val ft = df.where("a.device_id IS NULL AND b.device_id IS NOT NULL")
      //  val rdd = sc.cassandraTable("dmp", "device_target").select("device_id", "age")
      //  val new_rdd = sc.cassandraTable("test", "message")

      //  rdd.foreach(println)
      //  rdd.leftJoinWithCassandraTable("test", "device").foreach(println)

      //  val df = sc.parallelize(1 to 10).map(id => {
      //  Device("" + UUID.randomUUID())
      //  })

      //  df.saveToCassandra("test", "device")

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }

  def addValue: UserDefinedFunction = udf((array: Seq[String]) => {
    (array ++ Array(offer_id)).distinct
  })

  def subValue: UserDefinedFunction = udf((array: Seq[String]) => {
    val b = array.toBuffer
    b -= offer_id
    b
  })
}

object CassandraJoinExample {
  def main(args: Array[String]): Unit = {
    new CassandraJoinExample().run(args)
  }
}