package com.jinfeng.spark.example.sql

import org.apache.spark.sql.SparkSession

/**
  * @package: com.jinfeng.spark.example.sql
  * @author: wangjf
  * @date: 2019/5/23
  * @time: 下午5:10
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
object TestUrlSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TestUrlSpark")
      .master("local[5]")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      val input = "s3://cdn-adn.rayjump.com/cdn-adn/portal/17/07/26/16/48/597857714c322.txt"
      val rdd = sc.textFile(input)
      rdd.take(10)

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }

}
