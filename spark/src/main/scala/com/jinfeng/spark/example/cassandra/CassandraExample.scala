package com.jinfeng.spark.example.cassandra

import java.io.Serializable
import java.util

import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * @package: com.jinfeng.spark.example.cassandra
  * @author: wangjf
  * @date: 2019/3/29
  * @time: 下午12:00
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
case class Address(city: String, street: String, number: Int)
case class CompanyRow(name: String, address: util.ArrayList[Address])
class CassandraExample extends Serializable {
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

      //  val rdd = sc.cassandraTable("dmp", "device_target")
      //  val new_rdd = sc.cassandraTable("test", "message")
      //  rdd.foreach(println)
      //  rdd.joinWithCassandraTable("test", "message").foreach(println)


      //  val a = System.currentTimeMillis
      //  val df = sc.parallelize(1 to 10000).map(f = id => {
        //  DeviceOffer("" + UUID.randomUUID(), null, mutable.WrappedArray.empty, null, mutable.WrappedArray.empty, null, mutable.WrappedArray.empty)
        //  Device("" + UUID.randomUUID())
      //  })

      //  df.saveToCassandra("dmp", "device_target")
      //  df.saveToCassandra("test", "device")

      /*
      val address1 = Address(city = "Oakland", street = "Broadway", number = 3400)
      val address2 = Address(city = "Oakland", street = "Broadway", number = 3400)
      val list:java.util.ArrayList[Address] = new util.ArrayList[Address]()
      list.add(address1)
      list.add(address2)
      sc.parallelize(Seq(CompanyRow("Paul", list))).saveToCassandra("test", "companies")
      */

      //  , writeConf = WriteConf(ttl = TTLOption.constant(100)))
      //  val b = System.currentTimeMillis
      //  println("Spark Run Time == " + (b - a))
      //  println(rdd.count)
      //  println(rdd.first)
      //  println(rdd.map(_.getInt("value")).sum)

      val collection = sc.parallelize(Seq(("key3", Set("A")), ("key4", new java.util.HashSet[String]())))
      collection.saveToCassandra("dmp", "recent_device_region", SomeColumns("devid", "region"))

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}

object CassandraExample {
  def main(args: Array[String]): Unit = {
    new CassandraExample().run(args)
  }
}