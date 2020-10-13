package com.jinfeng.spark.example.example

import com.alibaba.fastjson.{JSONArray, JSONObject}
import org.apache.spark.sql.SparkSession

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
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.parallelize(List((1, 1), (3, 1), (4, 1), (1, 1), (3, 1), (4, 1), (1, 1), (3, 1), (4, 1), (1, 1), (3, 1), (4, 1), (1, 1), (3, 1)))
      .combineByKey(
        (v: Int) => v,
        (c: Int, v: Int) => c + v,
        (c1: Int, c2: Int) => c1 + c2
      )
    rdd.foreach(println)
    //  rdd.take(10).foreach(println)
    //  val map = new mutable.HashMap[Int, Int]()
    /*
    val jsonArray = new JSONArray()
    val df = rdd.map(r => {
      //  val jsonObject = new JSONObject()
      //  jsonObject.put(String.valueOf(r._1), r._3)
      //  jsonArray.add(jsonObject)
      (r._1, r._2)
    }).cache()
    df.collect().foreach(r=>{
      val jsonObject = new JSONObject()
      jsonObject.put(String.valueOf(r._1), r._2)
      jsonArray.add(jsonObject)
    })

    println(jsonArray.size)
    */

    //  df.take(10).foreach(println)
    sc.stop()
  }
}

object RTDmpMain {
  def main(args: Array[String]): Unit = {
    new RTDmpMain().run()
  }
}