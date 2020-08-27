package com.jinfeng.spark.example.example

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * @package: com.jinfeng.spark.example.example
 * @author: wangjf
 * @date: 2020/7/13
 * @time: 3:51 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
class RTDmpMain {
  def run(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("ThreadSpark")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()
    val sc = spark.sparkContext

    /*
    val rdd = sc.parallelize(List((1, 2, 3), (3, 4, 5), (4, 5, 6)))
    rdd.take(10).foreach(println)
    val map = new mutable.HashMap[Int, Int]()
    val df = rdd.map(r => {
      map.put(r._1, r._3)
      (r._1, r._2)
    })

    println(map.size)
    */
    val rdd = sc.textFile("/Users/wangjf/Workspace/data/device/*/*")
    rdd.take(10).foreach(println)
    sc.stop()
  }
}

object RTDmpMain {
  def main(args: Array[String]): Unit = {
    new RTDmpMain().run()
  }
}