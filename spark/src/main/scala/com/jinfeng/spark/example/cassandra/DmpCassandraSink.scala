package com.jinfeng.spark.example.cassandra

import com.datastax.spark.connector.{SomeColumns, _}
import com.jinfeng.spark.example.entity.DeviceTarget
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * @package: com.jinfeng.spark.example.cassandra
  * @author: wangjf
  * @date: 2019-06-19
  * @time: 15:30
  * @emial: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
object DmpCassandraSink {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[8]")
      .appName("DmpCassandraSink")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()

    try {

      val sc = spark.sparkContext

      val map = new mutable.HashMap[Integer, (String, Int)]()
      map.put(293058636, ("/Users/wangjf/Workspace/data/293058636.txt", 1))

      var rdd = sc.emptyRDD[(String, Integer)]
      map.foreach(m => {
        if (m._2._2 == 1) {
          rdd = rdd.union(sc.textFile(m._2._1).map(r => {
            (r, m._1)
          }))
        } else {
          rdd = rdd.union(sc.textFile(m._2._1).map(r => {
            (r, 0)
          }))
        }
      })
      //  import scala.collection.JavaConverters._
      val rdd1 = rdd.combineByKey(
        (v: Integer) => Iterable(v),
        (c: Iterable[Integer], v: Integer) => c ++ Seq(v),
        (c1: Iterable[Integer], c2: Iterable[Integer]) => c1 ++ c2
      )

      val df = rdd1.map(r => {
        DeviceTarget(r._1, r._2.toSet)
      })

      df.saveToCassandra("dmp_realtime_service", "dmp_user_features", SomeColumns("device_id", "target_offer_list" overwrite))

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}
