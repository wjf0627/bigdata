package com.jinfeng.spark.example.example

import org.apache.spark.sql.SparkSession

/**
 * @package: com.jinfeng.spark.example.example
 * @author: wangjf
 * @date: 2020/7/13
 * @time: 3:51 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
object SparkListExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("ThreadSpark")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .getOrCreate()
    val sc = spark.sparkContext

    SparkUtil.listDirs("/")
    sc.stop()
  }
}

