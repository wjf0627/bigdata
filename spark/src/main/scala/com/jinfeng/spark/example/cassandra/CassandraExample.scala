package com.jinfeng.spark.example.cassandra

import java.io.Serializable
import java.util

import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession

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
      //  val collection = sc.parallelize(Seq(("key3",Set("A")), ("key5",Set("B"))))
      val collection = sc.parallelize(Seq("key3", "key5"))
      //  val df = collection.toDF("devid", "region").rdd
      collection.joinWithCassandraTable("dmp", "recent_device_region",SomeColumns("devid"))
        .collect()
        .foreach(println)
      //  collection.saveToCassandra("dmp", "recent_device_region", SomeColumns("devid", "region"))
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