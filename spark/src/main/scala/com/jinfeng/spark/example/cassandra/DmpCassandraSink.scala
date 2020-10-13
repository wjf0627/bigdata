package com.jinfeng.spark.example.cassandra

import java.util
import java.util.List
import java.util.concurrent.CompletionStage

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures
import com.datastax.oss.driver.shaded.guava.common.collect.Lists
import com.datastax.spark.connector.cql.CassandraConnector
import com.jinfeng.util.{CassandraUtil, Demo}
import org.apache.spark.sql.SparkSession

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

      /*
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
      */

      //  import spark.implicits._
      /*
      val df = sc.parallelize(1 to 100).map(f = id => {
        Row("" + UUID.randomUUID(), new mutable.HashSet[String]().toSet)
      })

      df.saveToCassandra("rtdmp", "recent_device_region", SomeColumns("devid", "region"))
      */

      import scala.collection.JavaConversions._
      val cassandraConnector = CassandraConnector(sc.getConf)
      //  List<CompletionStage<Demo>> completionStages = Lists.newArrayList();
      val rdd = sc.parallelize(scala.List("1f23d864-3fb7-470b-9912-ebd4e681ebaf", "fa5004c4-fed8-4368-9b6f-4d163c9d5d44", "9cda161f-da05-4fb0-9c19-90a122539032"))
        .mapPartitions(rs => {
          val array = new util.ArrayList[String]()
          rs.foreach(r => {
            val cqlSession = cassandraConnector.openSession()
            val resultStage = CassandraUtil.testV4(cqlSession, r)
            /*
            resultStage.whenComplete((_: Demo, _: Throwable) =>
              cqlSession.close())
            */
            array.add(resultStage.get().getDevid)
          })
          array.iterator()
        })

      rdd.foreach(println)
      //  val set = new mutable.HashSet[String]()
      //  set
      //  val array = mutable.WrappedArray.make(set)
      //  println(array.toSet)

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}
